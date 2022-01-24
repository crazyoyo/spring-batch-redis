package com.redis.spring.batch.builder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.support.AbstractValueReader.ValueReaderBuilder;
import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveKeyItemReader;
import com.redis.spring.batch.support.LiveRedisClusterKeyItemReader;
import com.redis.spring.batch.support.LiveRedisItemReader;
import com.redis.spring.batch.support.LiveRedisKeyItemReader;
import com.redis.spring.batch.support.ScanKeyItemReader;
import com.redis.spring.batch.support.ValueReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;

public class LiveRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ValueReader<String, T>>
		extends RedisItemReaderBuilder<T, R, LiveRedisItemReaderBuilder<T, R>> {

	public static final int DEFAULT_DATABASE = 0;
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	private static final Converter<String, String> STRING_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);

	private Duration flushingInterval = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	private Duration idleTimeout;
	private List<String> keyPatterns = Arrays.asList(ScanKeyItemReader.DEFAULT_SCAN_MATCH);
	private int database = DEFAULT_DATABASE;
	private int notificationQueueCapacity = LiveKeyItemReader.DEFAULT_QUEUE_CAPACITY;

	public LiveRedisItemReaderBuilder<T, R> flushingInterval(Duration flushingInterval) {
		this.flushingInterval = flushingInterval;
		return this;
	}

	public LiveRedisItemReaderBuilder<T, R> idleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
		return this;
	}

	public LiveRedisItemReaderBuilder<T, R> database(int database) {
		this.database = database;
		return this;
	}

	public LiveRedisItemReaderBuilder<T, R> notificationQueueCapacity(int notificationQueueCapacity) {
		this.notificationQueueCapacity = notificationQueueCapacity;
		return this;
	}

	public LiveRedisItemReaderBuilder(AbstractRedisClient client,
			ValueReaderBuilder<String, String, T, R> valueReaderFactory) {
		super(client, valueReaderFactory);
	}

	public LiveRedisItemReaderBuilder<T, R> keyPatterns(String... keyPatterns) {
		this.keyPatterns = Arrays.asList(keyPatterns);
		return this;
	}

	public LiveRedisItemReader<String, T> build() {
		LiveRedisItemReader<String, T> reader = new LiveRedisItemReader<>(jobRepository, transactionManager,
				keyReader(), valueReader());
		reader.setFlushingInterval(flushingInterval);
		reader.setIdleTimeout(idleTimeout);
		return configure(reader);
	}

	public LiveKeyItemReader<String> keyReader() {
		LiveKeyItemReader<String> reader = liveKeyItemReader();
		reader.setQueueCapacity(notificationQueueCapacity);
		return reader;
	}

	public static List<String> pubSubPatterns(int database, String... keyPatterns) {
		List<String> patterns = new ArrayList<>();
		for (String keyPattern : keyPatterns) {
			patterns.add(pubSubPattern(database, keyPattern));
		}
		return patterns;
	}

	private List<String> pubSubPatterns() {
		return pubSubPatterns(database, keyPatterns.toArray(new String[0]));
	}

	public static String pubSubPattern(int database, String keyPattern) {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

	private LiveKeyItemReader<String> liveKeyItemReader() {
		if (client instanceof RedisClusterClient) {
			return new LiveRedisClusterKeyItemReader<>(((RedisClusterClient) client)::connectPubSub,
					STRING_KEY_EXTRACTOR, pubSubPatterns());
		}
		return new LiveRedisKeyItemReader<>(((RedisClient) client)::connectPubSub, STRING_KEY_EXTRACTOR,
				pubSubPatterns());
	}

}