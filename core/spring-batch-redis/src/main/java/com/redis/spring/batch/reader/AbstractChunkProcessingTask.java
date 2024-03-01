package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.redis.spring.batch.util.IdentityOperator;

public abstract class AbstractChunkProcessingTask<K, V> implements ProcessingTask {

	public static final int DEFAULT_CHUNK_SIZE = 50;

	private final Function<Iterable<K>, Iterable<V>> valueReader;
	private final BlockingQueue<V> valueQueue;
	private UnaryOperator<K> keyOperator = new IdentityOperator<>();
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private final List<K> chunk = new ArrayList<>();
	private long flushed = 0;
	private long added = 0;

	protected AbstractChunkProcessingTask(Function<Iterable<K>, Iterable<V>> valueReader, BlockingQueue<V> valueQueue) {
		this.valueReader = valueReader;
		this.valueQueue = valueQueue;
	}

	public void setChunkSize(int size) {
		this.chunkSize = size;
	}

	public void setKeyOperator(UnaryOperator<K> operator) {
		this.keyOperator = operator;
	}

	@Override
	public Long call() throws Exception {
		execute();
		flush();
		return flushed;
	}

	protected abstract void execute() throws InterruptedException;

	protected void add(K key) throws InterruptedException {
		chunk.add(key);
		added++;
		if (chunk.size() >= chunkSize) {
			flush();
		}
	}

	protected synchronized void flush() throws InterruptedException {
		List<K> processedKeys = chunk.stream().map(keyOperator).collect(Collectors.toList());
		Iterable<V> values = valueReader.apply(processedKeys);
		for (V value : values) {
			valueQueue.put(value);
			flushed++;
		}
		chunk.clear();
	}

}
