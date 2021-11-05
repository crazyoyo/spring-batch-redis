package com.redis.spring.batch.builder;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.ScanKeyItemReader;
import com.redis.spring.batch.support.ValueReader;

import io.lettuce.core.AbstractRedisClient;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class ScanRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ValueReader<String, T>>
		extends RedisItemReaderBuilder<T, R, ScanRedisItemReaderBuilder<T, R>> {

	private String scanMatch = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
	private long scanCount = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
	private String scanType;

	public ScanRedisItemReaderBuilder(AbstractRedisClient client,
			ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(client, valueReaderFactory);
	}

	public RedisItemReader<String, T> build() {
		ScanKeyItemReader<String, String> keyReader = new ScanKeyItemReader<>(connectionSupplier(), sync());
		keyReader.setCount(scanCount);
		keyReader.setMatch(scanMatch);
		keyReader.setType(scanType);
		return configure(new RedisItemReader<>(jobRepository, transactionManager, keyReader, valueReader()));
	}

	public LiveRedisItemReaderBuilder<T, R> live() {
		LiveRedisItemReaderBuilder<T, R> live = new LiveRedisItemReaderBuilder<>(client, valueReaderFactory);
		live.keyPatterns(scanMatch);
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