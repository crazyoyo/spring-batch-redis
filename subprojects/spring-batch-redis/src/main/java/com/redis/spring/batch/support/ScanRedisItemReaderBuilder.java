package com.redis.spring.batch.support;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.support.job.JobFactory;

import io.lettuce.core.AbstractRedisClient;

public class ScanRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>>
		extends RedisItemReaderBuilder<T, R, ScanRedisItemReaderBuilder<T, R>> {

	public static final String DEFAULT_SCAN_MATCH = "*";
	public static final long DEFAULT_SCAN_COUNT = 1000;

	private String scanMatch = DEFAULT_SCAN_MATCH;
	private long scanCount = DEFAULT_SCAN_COUNT;
	private String scanType;

	public ScanRedisItemReaderBuilder(JobFactory jobFactory, AbstractRedisClient client,
			ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(jobFactory, client, valueReaderFactory);
	}

	public ScanRedisItemReaderBuilder<T, R> scanMatch(String scanMatch) {
		this.scanMatch = scanMatch;
		return this;
	}

	public ScanRedisItemReaderBuilder<T, R> scanCount(long scanCount) {
		this.scanCount = scanCount;
		return this;
	}

	public ScanRedisItemReaderBuilder<T, R> scanType(String scanType) {
		this.scanType = scanType;
		return this;
	}

	public ItemReader<String> keyReader() {
		return new ScanKeyItemReader(connectionSupplier(), sync(), scanMatch, scanCount, scanType);
	}

	public RedisItemReader<String, T> build() {
		return new RedisItemReader<>(jobFactory, keyReader(), valueReader(), threads, chunkSize, queue(),
				queuePollTimeout, skipPolicy);
	}

	public LiveRedisItemReaderBuilder<T, R> live() {
		return new LiveRedisItemReaderBuilder<>(jobFactory, client, valueReaderFactory);
	}
}