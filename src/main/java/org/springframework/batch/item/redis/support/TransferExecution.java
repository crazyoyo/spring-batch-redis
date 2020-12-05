package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.core.metrics.BatchMetrics;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransferExecution<I, O> {

	@Getter
	private final Transfer<I, O> transfer;
	private final Collection<TransferExecutionListener> listeners = new ArrayList<>();

	public void addListener(TransferExecutionListener listener) {
		listeners.add(listener);
	}

	private Collection<TransferWorker> workers;
	private AtomicLong count = new AtomicLong();
	private boolean stopped;

	public TransferExecution(Transfer<I, O> transfer) {
		this.transfer = transfer;
	}

	public CompletableFuture<Void> start() {
		this.stopped = false;
		this.workers = new ArrayList<>(transfer.getOptions().getThreads());
		for (int index = 0; index < transfer.getOptions().getThreads(); index++) {
			workers.add(transfer.getProcessor() == null ? new TransferWorker() : new ProcessingTransferWorker());
		}
		ExecutionContext executionContext = new ExecutionContext();
		if (transfer.getReader() instanceof ItemStream) {
			log.debug("{}: opening reader", transfer.getName());
			((ItemStream) transfer.getReader()).open(executionContext);
		}
		if (transfer.getWriter() instanceof ItemStream) {
			log.debug("{}: opening writer", transfer.getName());
			((ItemStream) transfer.getWriter()).open(executionContext);
		}
		Collection<CompletableFuture<Void>> futures = new ArrayList<>();
		for (TransferWorker worker : workers) {
			futures.add(CompletableFuture.runAsync(worker));
		}
		CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
		future.whenComplete((v, t) -> {
			if (t != null) {
				listeners.forEach(l -> l.onError(t));
			}
			try {
				if (transfer.getWriter() instanceof ItemStream) {
					log.debug("{}: closing writer", transfer.getName());
					((ItemStream) transfer.getWriter()).close();
				}
				if (transfer.getReader() instanceof ItemStream) {
					log.debug("{}: closing reader", transfer.getName());
					((ItemStream) transfer.getReader()).close();
				}
			} catch (Exception e) {
				listeners.forEach(l -> l.onError(e));
			} finally {
				listeners.forEach(TransferExecutionListener::onComplete);
			}
		});
		if (transfer.getOptions().getFlushInterval() != null) {
			ScheduledFuture<?> flushFuture = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
					this::flush, 0, transfer.getOptions().getFlushInterval().toMillis(), TimeUnit.MILLISECONDS);
			future.whenComplete((v, t) -> flushFuture.cancel(true));
		}
		return future;
	}

	public void flush() {
		if (isTerminated()) {
			return;
		}
		if (transfer.getReader() instanceof FlushableItemStreamReader) {
			((FlushableItemStreamReader<I>) transfer.getReader()).flush();
		}
		workers.forEach(TransferWorker::write);
	}

	public boolean isTerminated() {
		for (TransferWorker worker : workers) {
			if (worker.isRunning()) {
				return false;
			}
		}
		return true;
	}

	public void stop() throws InterruptedException, ExecutionException {
		this.stopped = true;
		log.debug("{} stopped", transfer.getName());
	}

	private class TransferWorker implements Runnable {

		private static final String READ_TIMER_NAME = "item.read";
		private static final String READ_TIMER_DESCRIPTION = "Item reading duration";
		private static final String WRITE_TIMER_NAME = "chunk.write";
		private static final String WRITE_TIMER_DESCRIPTION = "Chunk writing duration";
		private static final String STATUS_TAG_NAME = "status";

		protected final Tag nameTag = Tag.of("transfer.name", transfer.getName());
		private final Object lock = new Object();
		private List<O> items = new ArrayList<>(transfer.getOptions().getBatch());
		@Getter
		private boolean running;

		@Override
		public void run() {
			this.running = true;
			do {
				I item;
				try {
					item = read();
				} catch (Exception e) {
					log.error("{}: could not read next item", transfer.getName(), e);
					continue;
				}
				if (item == null) {
					break;
				}
				try {
					O processed = process(item);
					synchronized (lock) {
						items.add(processed);
					}
				} catch (Exception e) {
					log.error("{}: could not process item", transfer.getName(), e);
					continue;
				}
				if (items.size() >= transfer.getOptions().getBatch()) {
					write();
				}
			} while (!stopped);
			if (!stopped) {
				write();
			}
			log.debug("{}: worker completed", transfer.getName());
			this.running = false;
		}

		@SuppressWarnings("unchecked")
		protected O process(I item) throws Exception {
			return (O) item;
		}

		protected Tag statusTag(String status) {
			return Tag.of(STATUS_TAG_NAME, status);
		}

		private I read() throws Exception {
			Timer.Sample sample = MetricsUtils.createTimerSample();
			String status = BatchMetrics.STATUS_SUCCESS;
			try {
				return transfer.getReader().read();
			} catch (Exception e) {
				status = BatchMetrics.STATUS_FAILURE;
				throw e;
			} finally {
				sample.stop(
						MetricsUtils.createTimer(READ_TIMER_NAME, READ_TIMER_DESCRIPTION, nameTag, statusTag(status)));
			}
		}

		public void write() {
			List<O> batch;
			synchronized (lock) {
				batch = items;
				items = new ArrayList<>(transfer.getOptions().getBatch());
			}
			Timer.Sample sample = MetricsUtils.createTimerSample();
			String status = BatchMetrics.STATUS_SUCCESS;
			try {
				transfer.getWriter().write(batch);
				long totalCount = count.addAndGet(batch.size());
				listeners.forEach(l -> l.onUpdate(totalCount));
			} catch (Exception e) {
				status = BatchMetrics.STATUS_FAILURE;
				log.error("{}: could not write items", transfer.getName(), e);
			} finally {
				sample.stop(MetricsUtils.createTimer(WRITE_TIMER_NAME, WRITE_TIMER_DESCRIPTION, nameTag,
						statusTag(status)));
			}
		}

	}

	public long count() {
		return count.get();
	}

	private class ProcessingTransferWorker extends TransferWorker {

		private static final String PROCESS_TIMER_NAME = "item.process";
		private static final String PROCESS_TIMER_DESCRIPTION = "Item processing duration";

		@Override
		protected O process(I item) throws Exception {
			Timer.Sample sample = MetricsUtils.createTimerSample();
			String status = BatchMetrics.STATUS_SUCCESS;
			try {
				return transfer.getProcessor().process(item);
			} catch (Exception e) {
				status = BatchMetrics.STATUS_FAILURE;
				throw e;
			} finally {
				sample.stop(MetricsUtils.createTimer(PROCESS_TIMER_NAME, PROCESS_TIMER_DESCRIPTION, nameTag,
						statusTag(status)));
			}
		}

	}

}