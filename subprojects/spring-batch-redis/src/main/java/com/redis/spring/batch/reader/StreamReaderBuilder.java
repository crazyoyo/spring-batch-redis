package com.redis.spring.batch.reader;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.ClassUtils;

import io.lettuce.core.Consumer;
import io.lettuce.core.api.StatefulConnection;

public class StreamReaderBuilder<K, V> {

	public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(StreamItemReader.class);
	public static final String DEFAULT_CONSUMER = "consumer1";

	private final GenericObjectPool<StatefulConnection<K, V>> connectionPool;
	private final K stream;
	private final Consumer<K> consumer;
	private StreamReaderOptions options = StreamReaderOptions.builder().build();

	public StreamReaderBuilder(GenericObjectPool<StatefulConnection<K, V>> connectionPool, K stream,
			Consumer<K> consumer) {
		this.connectionPool = connectionPool;
		this.stream = stream;
		this.consumer = consumer;
	}

	public StreamReaderBuilder<K, V> options(StreamReaderOptions options) {
		this.options = options;
		return this;
	}

	public StreamItemReader<K, V> build() {
		return new StreamItemReader<>(connectionPool, stream, consumer, options);
	}

}