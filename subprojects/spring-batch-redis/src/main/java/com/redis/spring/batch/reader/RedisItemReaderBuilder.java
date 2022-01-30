package com.redis.spring.batch.reader;

import java.time.Duration;

import org.springframework.batch.core.step.skip.SkipPolicy;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.AbstractValueReader.ValueReaderBuilder;
import com.redis.spring.batch.support.JobRepositoryBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemReaderBuilder<K, V, T extends KeyValue<K, ?>, R extends ValueReader<K, T>, B extends RedisItemReaderBuilder<K, V, T, R, B>>
		extends JobRepositoryBuilder<K, V, B> {

	protected final ValueReaderBuilder<K, V, T, R> valueReaderFactory;
	protected int threads = RedisItemReader.DEFAULT_THREADS;
	protected int chunkSize = RedisItemReader.DEFAULT_CHUNK_SIZE;
	protected int valueQueueCapacity = RedisItemReader.DEFAULT_QUEUE_CAPACITY;
	protected Duration queuePollTimeout = RedisItemReader.DEFAULT_QUEUE_POLL_TIMEOUT;
	protected SkipPolicy skipPolicy = RedisItemReader.DEFAULT_SKIP_POLICY;

	public RedisItemReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
			ValueReaderBuilder<K, V, T, R> valueReaderFactory) {
		super(client, codec);
		this.valueReaderFactory = valueReaderFactory;
	}

	@SuppressWarnings("unchecked")
	public B threads(int threads) {
		this.threads = threads;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B chunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B valueQueueCapacity(int queueCapacity) {
		this.valueQueueCapacity = queueCapacity;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B queuePollTimeout(Duration queuePollTimeout) {
		this.queuePollTimeout = queuePollTimeout;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B skipPolicy(SkipPolicy skipPolicy) {
		this.skipPolicy = skipPolicy;
		return (B) this;
	}

	@SuppressWarnings("rawtypes")
	protected <X extends RedisItemReader> X configure(X reader) {
		reader.setChunkSize(chunkSize);
		reader.setQueueCapacity(valueQueueCapacity);
		reader.setQueuePollTimeout(queuePollTimeout);
		reader.setSkipPolicy(skipPolicy);
		reader.setThreads(threads);
		return reader;
	}

	protected R valueReader() {
		return valueReaderFactory.create(connectionSupplier(), poolConfig, async());
	}

}