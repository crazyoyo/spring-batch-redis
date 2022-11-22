package com.redis.spring.batch.reader;

import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.StepOptions;

import io.lettuce.core.api.StatefulConnection;

public class ScanReaderBuilder<K, V, T extends KeyValue<K>> {

	private final GenericObjectPool<StatefulConnection<K, V>> connectionPool;
	private final JobRunner jobRunner;
	private final ItemProcessor<List<? extends K>, List<T>> valueReader;
	private StepOptions stepOptions = StepOptions.builder().build();
	private QueueOptions queueOptions = QueueOptions.builder().build();
	private ScanOptions scanOptions = ScanOptions.builder().build();

	public ScanReaderBuilder(GenericObjectPool<StatefulConnection<K, V>> connectionPool, JobRunner jobRunner,
			ItemProcessor<List<? extends K>, List<T>> valueReader) {
		this.connectionPool = connectionPool;
		this.jobRunner = jobRunner;
		this.valueReader = valueReader;
	}

	public ScanReaderBuilder<K, V, T> stepOptions(StepOptions options) {
		this.stepOptions = options;
		return this;
	}

	public ScanReaderBuilder<K, V, T> queueOptions(QueueOptions options) {
		this.queueOptions = options;
		return this;
	}

	public ScanReaderBuilder<K, V, T> scanOptions(ScanOptions options) {
		this.scanOptions = options;
		return this;
	}

	public RedisItemReader<K, T> build() {
		return new RedisItemReader<>(jobRunner, keyReader(), valueReader, stepOptions, queueOptions);
	}

	public ScanKeyItemReader<K, V> keyReader() {
		return new ScanKeyItemReader<>(connectionPool, scanOptions);
	}

}