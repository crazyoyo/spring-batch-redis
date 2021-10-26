package com.redis.spring.batch.support;

import java.util.List;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;

import io.lettuce.core.AbstractRedisClient;

public class ScanRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>>
		extends RedisItemReaderBuilder<T, R, ScanRedisItemReaderBuilder<T, R>> {

	public static final String DEFAULT_SCAN_MATCH = "*";
	public static final long DEFAULT_SCAN_COUNT = 1000;

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private String scanMatch = DEFAULT_SCAN_MATCH;
	private long scanCount = DEFAULT_SCAN_COUNT;
	private String scanType;

	public ScanRedisItemReaderBuilder(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			AbstractRedisClient client, ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(client, valueReaderFactory);
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
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
		return new RedisItemReader<>(jobRepository, transactionManager, keyReader(), valueReader(), threads, chunkSize,
				queue(), queuePollTimeout, skipPolicy);
	}

	public LiveRedisItemReaderBuilder<T, R> live() {
		return new LiveRedisItemReaderBuilder<>(jobRepository, transactionManager, client, valueReaderFactory);
	}
}