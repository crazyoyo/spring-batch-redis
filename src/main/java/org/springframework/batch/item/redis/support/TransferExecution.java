package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.Worker.WorkerListener;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransferExecution<I, O> implements WorkerListener {

	private final Transfer<I, O> transfer;
	private final Collection<Worker<I>> workers;
	@Getter
	private final CompletableFuture<Void> future;
	private final Collection<TransferExecutionListener> listeners = new ArrayList<>();
	private final AtomicLong count = new AtomicLong();

	public TransferExecution(Transfer<I, O> transfer) {
		this.transfer = transfer;
		ExecutionContext executionContext = new ExecutionContext();
		ItemReader<I> reader = reader();
		if (reader instanceof ItemStream) {
			((ItemStream) reader).open(executionContext);
		}
		ItemWriter<I> writer = writer();
		if (writer instanceof ItemStream) {
			((ItemStream) writer).open(executionContext);
		}
		this.workers = new ArrayList<>(transfer.getThreads());
		for (int index = 0; index < transfer.getThreads(); index++) {
			workers.add(new Worker<>(transfer.getName(), reader, writer, transfer.getBatch()));
		}
		Collection<CompletableFuture<Void>> futures = new ArrayList<>();
		for (Worker<?> worker : workers) {
			worker.addListener(this);
			futures.add(CompletableFuture.runAsync(worker));
		}
		this.future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
		if (transfer.getFlushInterval() != null) {
			ScheduledFuture<?> flushFuture = Executors.newSingleThreadScheduledExecutor()
					.scheduleAtFixedRate(this::flush, 0, transfer.getFlushInterval().toMillis(), TimeUnit.MILLISECONDS);
			future.whenComplete((v, t) -> flushFuture.cancel(true));
		}
		future.whenComplete((v, t) -> {
			if (reader instanceof ItemStream) {
				log.debug("{}: closing reader", transfer.getName());
				((ItemStream) reader).close();
			}
			if (writer instanceof ItemStream) {
				log.debug("{}: closing writer", transfer.getName());
				((ItemStream) writer).close();
			}
			listeners.forEach(TransferExecutionListener::onComplete);
			if (t != null) {
				listeners.forEach(l -> l.onError(t));
			}
		});
	}

	private ItemReader<I> reader() {
		if (transfer.getThreads() > 1 && transfer.getReader() instanceof ItemStreamReader) {
			if (transfer.getReader() instanceof FlushableItemStreamReader) {
				FlushableSynchronizedItemStreamReader<I> flushableReader = new FlushableSynchronizedItemStreamReader<>();
				flushableReader.setDelegate((FlushableItemStreamReader<I>) transfer.getReader());
				return flushableReader;
			}
			SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
			synchronizedReader.setDelegate((ItemStreamReader<I>) transfer.getReader());
			return synchronizedReader;
		}
		return transfer.getReader();
	}

	@SuppressWarnings("unchecked")
	private ItemWriter<I> writer() {
		if (transfer.getProcessor() == null) {
			return (ItemWriter<I>) transfer.getWriter();
		}
		return new ProcessingItemWriter<>(transfer.getProcessor(), transfer.getWriter());
	}

	public void addListener(TransferExecutionListener listener) {
		this.listeners.add(listener);
	}

	public void flush() {
		if (transfer.getReader() instanceof FlushableItemStreamReader) {
			((FlushableItemStreamReader<I>) transfer.getReader()).flush();
		}
		workers.forEach(Worker::flush);
	}

	@Override
	public void onWrite(long writeCount) {
		long totalCount = count.addAndGet(writeCount);
		listeners.forEach(l -> l.onProgress(totalCount));
	}

	public boolean isRunning() {
		return !future.isDone() && !future.isCancelled() && !future.isCompletedExceptionally();
	}

	public void stop() {
		workers.forEach(Worker::stop);
	}

}