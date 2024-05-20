package com.redis.spring.batch.item.redis.reader;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.item.redis.RedisItemReader;

public class KeyComparisonItemReader<K, V> extends AbstractAsyncItemReader<MemKeyValue<K, Object>, KeyComparison<K>> {

	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	private final RedisItemReader<K, V, MemKeyValue<K, Object>> sourceReader;
	private final RedisItemReader<K, V, MemKeyValue<K, Object>> targetReader;

	private KeyComparator<K, V> comparator = new DefaultKeyComparator<>();

	public KeyComparisonItemReader(RedisItemReader<K, V, MemKeyValue<K, Object>> sourceReader,
			RedisItemReader<K, V, MemKeyValue<K, Object>> targetReader) {
		this.sourceReader = sourceReader;
		this.targetReader = targetReader;
	}

	@Override
	protected boolean isFlushing() {
		return false;
	}

	public RedisItemReader<K, V, MemKeyValue<K, Object>> getSourceReader() {
		return sourceReader;
	}

	public RedisItemReader<K, V, MemKeyValue<K, Object>> getTargetReader() {
		return targetReader;
	}

	@Override
	protected ItemReader<MemKeyValue<K, Object>> reader() {
		return sourceReader;
	}

	@Override
	protected ItemProcessor<Iterable<? extends MemKeyValue<K, Object>>, List<KeyComparison<K>>> writeProcessor() {
		return new KeyComparisonItemProcessor<>(targetReader.operationExecutor(), comparator);
	}

	public KeyComparator<K, V> getComparator() {
		return comparator;
	}

	public void setComparator(KeyComparator<K, V> comparator) {
		this.comparator = comparator;
	}

}
