package com.redis.spring.batch.item.redis.reader;

import com.redis.spring.batch.item.redis.RedisItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.RedisCodec;

public class KeyComparisonItemReader<K, V> extends RedisItemReader<K, V, KeyComparison<K>> {

	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	private AbstractRedisClient targetClient;
	private int targetPoolSize = DEFAULT_TARGET_POOL_SIZE;
	private KeyComparatorOptions comparatorOptions = new KeyComparatorOptions();
	private ReadFrom targetReadFrom;

	public KeyComparisonItemReader(RedisCodec<K, V> codec, MemKeyValueRead<K, V, Object> source,
			MemKeyValueRead<K, V, Object> target) {
		super(codec, new KeyComparisonRead<>(codec, source, target));
	}

	public void setTargetClient(AbstractRedisClient client) {
		this.targetClient = client;
	}

	public void setTargetPoolSize(int size) {
		this.targetPoolSize = size;
	}

	public void setTargetReadFrom(ReadFrom readFrom) {
		this.targetReadFrom = readFrom;
	}

	public KeyComparatorOptions getComparatorOptions() {
		return comparatorOptions;
	}

	public void setComparatorOptions(KeyComparatorOptions options) {
		this.comparatorOptions = options;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		KeyComparisonRead<K, V> keyComparisonRead = (KeyComparisonRead<K, V>) operation;
		keyComparisonRead.setTargetClient(targetClient);
		keyComparisonRead.setTargetPoolSize(targetPoolSize);
		keyComparisonRead.setTargetReadFrom(targetReadFrom);
		keyComparisonRead.setComparator(comparator());
		super.doOpen();
	}

	private KeyComparator<K, V> comparator() {
		KeyComparator<K, V> comparator = new KeyComparator<>();
		comparator.setOptions(comparatorOptions);
		return comparator;
	}

}
