package com.redis.spring.batch.reader;

import java.io.IOException;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.Chunk;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.ValueReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractKeyValueItemReader<K, V> extends RedisItemReader<K, V, KeyValue<K>> {

	/**
	 * Default to no memory usage calculation
	 */
	public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = DataSize.ofBytes(0);
	public static final int DEFAULT_MEMORY_USAGE_SAMPLES = 5;
	public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

	private int poolSize = DEFAULT_POOL_SIZE;
	protected DataSize memLimit = DEFAULT_MEMORY_USAGE_LIMIT;
	protected int memSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

	private ValueReader<K, V, K, KeyValue<K>> valueReader;

	protected AbstractKeyValueItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client, codec);
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (valueReader == null) {
			valueReader = valueReader();
			valueReader.open();
		}
		super.doOpen();
	}

	@Override
	protected synchronized void doClose() throws Exception {
		super.doClose();
		if (valueReader != null) {
			valueReader.close();
		}
	}

	public ValueReader<K, V, K, KeyValue<K>> valueReader() throws IOException {
		ValueReader<K, V, K, KeyValue<K>> reader = new ValueReader<>(getClient(), getCodec(), operation());
		reader.setPoolSize(poolSize);
		reader.setReadFrom(getReadFrom());
		return reader;
	}

	@Override
	public Chunk<KeyValue<K>> values(Chunk<? extends K> chunk) {
		return valueReader.execute(chunk);
	}

	protected abstract Operation<K, V, K, KeyValue<K>> operation() throws IOException;

	public void setMemoryUsageLimit(DataSize limit) {
		this.memLimit = limit;
	}

	public void setMemoryUsageSamples(int samples) {
		this.memSamples = samples;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

}
