package org.springframework.batch.item.redis.support;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKeyValueItemReader<T extends KeyValue<?>> extends
		AbstractItemCountingItemStreamItemReader<T> implements BoundedItemReader<T>, FlushableItemStreamReader<T> {

	@Getter
	private final ItemReader<String> keyReader;
	private final ValueReader<T> valueReader;
	private final BlockingQueue<T> queue;
	private final Transfer<String, String> transfer;
	private Tag nameTag;
	private long queuePollingTimeout;
	private TransferExecution<String, String> transferExecution;
	private CompletableFuture<Void> transferFuture;

	protected AbstractKeyValueItemReader(ItemReader<String> keyReader, ValueReader<T> valueReader,
			TransferOptions transferOptions, QueueOptions queueOptions) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(keyReader, "A key reader is required.");
		Assert.notNull(valueReader, "A value reader is required.");
		Assert.notNull(transferOptions, "Transfer options are required.");
		Assert.notNull(queueOptions, "Queue options are required.");
		this.keyReader = keyReader;
		this.valueReader = valueReader;
		this.transfer = Transfer.<String, String>builder().name("value-reader").reader(keyReader)
				.writer(new ValueEnqueuer()).options(transferOptions).build();
		this.queue = new LinkedBlockingDeque<>(queueOptions.getCapacity());
		this.queuePollingTimeout = queueOptions.getPollingTimeout().toMillis();
	}

	@Override
	public void setName(String name) {
		this.nameTag = Tag.of("name", name);
		super.setName(name);
	}

	@Override
	protected void doOpen() {
		MetricsUtils.createGaugeCollectionSize("reader.queue.size", queue, nameTag);
		transferExecution = new TransferExecution<>(transfer);
		transferFuture = transferExecution.start();
	}

	@Override
	protected void doClose() throws ItemStreamException, InterruptedException, ExecutionException {
		if (!queue.isEmpty()) {
			log.warn("Closing {} - {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
		}
		log.info("Stopping key transfer");
		transferExecution.stop();
		log.info("Waiting for key transfer to finish");
		transferFuture.get();
	}

	private class ValueEnqueuer implements ItemStreamWriter<String> {

		@Override
		public void open(ExecutionContext executionContext) {
			valueReader.open(executionContext);
		}

		@Override
		public void close() throws ItemStreamException {
			valueReader.close();
		}

		@Override
		public void update(ExecutionContext executionContext) throws ItemStreamException {
			valueReader.update(executionContext);
		}

		@Override
		public void write(List<? extends String> keys) throws Exception {
			for (T value : valueReader.read(keys)) {
				queue.removeIf(v -> v.getKey().equals(value.getKey()));
				queue.put(value);
			}
		}

	}

	@Override
	protected T doRead() throws Exception {
		T item;
		do {
			item = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
		} while (item == null && !transferExecution.isTerminated());
		return item;
	}

	@Override
	public void flush() {
		transferExecution.flush();
	}

	@Override
	public int available() {
		if (keyReader instanceof BoundedItemReader) {
			return ((BoundedItemReader<String>) keyReader).available();
		}
		return 0;
	}

}
