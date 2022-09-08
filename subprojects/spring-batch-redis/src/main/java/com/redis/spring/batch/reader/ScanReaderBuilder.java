package com.redis.spring.batch.reader;

import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.api.StatefulConnection;

public class ScanReaderBuilder<K, V, T extends KeyValue<K>> {

	private final GenericObjectPool<StatefulConnection<K, V>> connectionPool;
	private final JobRunner jobRunner;
	private final ItemProcessor<List<? extends K>, List<T>> valueReader;
	private ScanReaderOptions options = ScanReaderOptions.builder().build();

	public ScanReaderBuilder(GenericObjectPool<StatefulConnection<K, V>> connectionPool, JobRunner jobRunner,
			ItemProcessor<List<? extends K>, List<T>> valueReader) {
		this.connectionPool = connectionPool;
		this.jobRunner = jobRunner;
		this.valueReader = valueReader;
	}

	public ScanReaderBuilder<K, V, T> options(ScanReaderOptions options) {
		this.options = options;
		return this;
	}

	public RedisItemReader<K, T> build() {
		return new RedisItemReader<>(keyReader(), valueReader, jobRunner, options);
	}

	public ScanKeyItemReader<K, V> keyReader() {
		return new ScanKeyItemReader<>(connectionPool, options);
	}

}