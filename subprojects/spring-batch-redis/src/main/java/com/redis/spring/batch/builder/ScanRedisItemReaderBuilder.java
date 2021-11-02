package com.redis.spring.batch.builder;

import java.util.List;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.ScanKeyItemReader;

import io.lettuce.core.AbstractRedisClient;

public class ScanRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>>
		extends RedisItemReaderBuilder<T, R, ScanRedisItemReaderBuilder<T, R>> {

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private String scanMatch = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
	private long scanCount = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
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

	public RedisItemReader<String, T> build() {
		ScanKeyItemReader<String, String> keyReader = new ScanKeyItemReader<>(connectionSupplier(), sync());
		keyReader.setCount(scanCount);
		keyReader.setMatch(scanMatch);
		keyReader.setType(scanType);
		return configure(new RedisItemReader<>(jobRepository, transactionManager, keyReader, valueReader()));
	}

	public LiveRedisItemReaderBuilder<T, R> live() {
		return new LiveRedisItemReaderBuilder<>(jobRepository, transactionManager, client, valueReaderFactory);
	}
}