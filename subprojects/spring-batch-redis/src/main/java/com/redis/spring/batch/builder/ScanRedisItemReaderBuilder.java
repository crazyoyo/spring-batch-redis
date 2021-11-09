package com.redis.spring.batch.builder;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.ScanKeyItemReader;
import com.redis.spring.batch.support.ValueReader;

import io.lettuce.core.AbstractRedisClient;

public class ScanRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ValueReader<String, T>>
		extends RedisItemReaderBuilder<T, R, ScanRedisItemReaderBuilder<T, R>> {

	private String match = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
	private long count = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
	private String type;

	public ScanRedisItemReaderBuilder(AbstractRedisClient client,
			ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(client, valueReaderFactory);
	}

	public ScanRedisItemReaderBuilder<T, R> match(String match) {
		this.match = match;
		return this;
	}

	public ScanRedisItemReaderBuilder<T, R> count(long count) {
		this.count = count;
		return this;
	}

	public ScanRedisItemReaderBuilder<T, R> type(String type) {
		this.type = type;
		return this;
	}

	public RedisItemReader<String, T> build() {
		ScanKeyItemReader<String, String> keyReader = new ScanKeyItemReader<>(connectionSupplier(), sync());
		keyReader.setCount(count);
		keyReader.setMatch(match);
		keyReader.setType(type);
		return configure(new RedisItemReader<>(jobRepository, transactionManager, keyReader, valueReader()));
	}

	public LiveRedisItemReaderBuilder<T, R> live() {
		LiveRedisItemReaderBuilder<T, R> live = new LiveRedisItemReaderBuilder<>(client, valueReaderFactory);
		live.keyPatterns(match);
		live.jobRepository(jobRepository);
		live.transactionManager(transactionManager);
		live.chunkSize(chunkSize);
		live.threads(threads);
		live.valueQueueCapacity(valueQueueCapacity);
		live.queuePollTimeout(queuePollTimeout);
		live.skipPolicy(skipPolicy);
		return live;
	}

}