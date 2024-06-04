package com.redis.spring.batch.memcached.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redis.spring.batch.item.AbstractPollableItemReader;
import com.redis.spring.batch.memcached.MemcachedException;
import com.redis.spring.batch.memcached.reader.LruCrawlerMetadumpOperation.Callback;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.ops.OperationStatus;

public class LruMetadumpItemReader extends AbstractPollableItemReader<LruMetadumpEntry> {

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private final Supplier<MemcachedClient> clientSupplier;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	protected BlockingQueue<LruMetadumpEntry> queue;

	public BlockingQueue<LruMetadumpEntry> getQueue() {
		return queue;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	private MemcachedClient client;
	private CountDownLatch latch;

	public LruMetadumpItemReader(Supplier<MemcachedClient> clientSupplier) {
		this.clientSupplier = clientSupplier;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueCapacity);
		}
		if (client == null) {
			client = clientSupplier.get();
			latch = client.broadcastOp(
					(n, l) -> new LruCrawlerMetadumpOperationImpl("all", new MetadumpCallback(l, this::safePut)));
		}
	}

	@Override
	protected LruMetadumpEntry doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	private void safePut(LruMetadumpEntry entry) {
		try {
			queue.put(entry);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new MemcachedException("Interrupted while trying to add entry to queue", e);
		}
	}

	@Override
	protected synchronized void doClose() throws Exception {
		if (client != null) {
			client.shutdown();
			client = null;
		}
		queue = null;
	}

	@Override
	public boolean isComplete() {
		return latch.getCount() == 0 && queue.isEmpty();
	}

	private static class MetadumpCallback implements Callback {

		private final Log log = LogFactory.getLog(getClass());

		private final CountDownLatch latch;
		private final Consumer<LruMetadumpEntry> consumer;

		public MetadumpCallback(CountDownLatch latch, Consumer<LruMetadumpEntry> consumer) {
			this.latch = latch;
			this.consumer = consumer;
		}

		@Override
		public void gotMetadump(LruMetadumpEntry entry) {
			consumer.accept(entry);
		}

		@Override
		public void receivedStatus(OperationStatus status) {
			if (!status.isSuccess()) {
				log.error("Unsuccessful lru_crawler metadump: " + status);
			}
		}

		@Override
		public void complete() {
			latch.countDown();
		}
	}

}
