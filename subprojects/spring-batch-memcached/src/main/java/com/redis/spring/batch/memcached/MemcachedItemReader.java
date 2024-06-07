package com.redis.spring.batch.memcached;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.item.ProcessingItemWriter;
import com.redis.spring.batch.item.BlockingQueueItemWriter;
import com.redis.spring.batch.memcached.reader.LruMetadumpEntry;
import com.redis.spring.batch.memcached.reader.LruMetadumpItemProcessor;
import com.redis.spring.batch.memcached.reader.LruMetadumpItemReader;

import net.spy.memcached.MemcachedClient;

public class MemcachedItemReader extends AbstractAsyncItemReader<LruMetadumpEntry, MemcachedEntry> {

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private final Supplier<MemcachedClient> clientSupplier;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private BlockingQueue<MemcachedEntry> queue;

	public MemcachedItemReader(Supplier<MemcachedClient> clientSupplier) {
		this.clientSupplier = clientSupplier;
	}

	@Override
	protected ItemReader<LruMetadumpEntry> reader() {
		return new LruMetadumpItemReader(clientSupplier);
	}

	@Override
	protected ItemWriter<LruMetadumpEntry> writer() {
		queue = new LinkedBlockingQueue<>(queueCapacity);
		return new ProcessingItemWriter<>(new LruMetadumpItemProcessor(clientSupplier), new BlockingQueueItemWriter<>(queue));
	}

	@Override
	protected MemcachedEntry doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

}
