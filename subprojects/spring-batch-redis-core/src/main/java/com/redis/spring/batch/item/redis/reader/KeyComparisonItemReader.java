package com.redis.spring.batch.item.redis.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.item.ProcessingItemWriter;
import com.redis.spring.batch.item.QueueItemWriter;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

public class KeyComparisonItemReader<K, V> extends AbstractAsyncItemReader<KeyValue<K, Object>, KeyComparison<K>> {

	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private final RedisItemReader<K, V, KeyValue<K, Object>> sourceReader;
	private final RedisItemReader<K, V, KeyValue<K, Object>> targetReader;

	private KeyComparator<K> comparator = new DefaultKeyComparator<>();
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private BlockingQueue<KeyComparison<K>> queue;

	public KeyComparisonItemReader(RedisItemReader<K, V, KeyValue<K, Object>> sourceReader,
			RedisItemReader<K, V, KeyValue<K, Object>> targetReader) {
		this.sourceReader = sourceReader;
		this.targetReader = targetReader;
	}

	@Override
	protected ItemWriter<KeyValue<K, Object>> writer() {
		queue = new LinkedBlockingQueue<>(queueCapacity);
		return new ProcessingItemWriter<>(processor(), new QueueItemWriter<>(queue));
	}

	private KeyComparisonItemProcessor<K, V, KeyValue<K, Object>> processor() {
		return new KeyComparisonItemProcessor<>(targetReader.operationExecutor(), comparator);
	}

	@Override
	protected KeyComparison<K> doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public RedisItemReader<K, V, KeyValue<K, Object>> getSourceReader() {
		return sourceReader;
	}

	public RedisItemReader<K, V, KeyValue<K, Object>> getTargetReader() {
		return targetReader;
	}

	@Override
	protected ItemReader<KeyValue<K, Object>> reader() {
		return sourceReader;
	}

	public KeyComparator<K> getComparator() {
		return comparator;
	}

	public void setComparator(KeyComparator<K> comparator) {
		this.comparator = comparator;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

}
