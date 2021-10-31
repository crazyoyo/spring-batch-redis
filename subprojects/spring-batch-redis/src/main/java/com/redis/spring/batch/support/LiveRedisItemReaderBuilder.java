package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiveRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>>
		extends RedisItemReaderBuilder<T, R, LiveRedisItemReaderBuilder<T, R>> {

	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_DATABASE = 0;
	public static final String DEFAULT_KEY_PATTERN = "*";
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	public static final List<String> DEFAULT_PUBSUB_PATTERNS = pubSubPatterns(DEFAULT_DATABASE, DEFAULT_KEY_PATTERN);
	private static final Converter<String, String> DEFAULT_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private String[] keyPatterns = new String[] { DEFAULT_KEY_PATTERN };
	private int database = DEFAULT_DATABASE;
	protected Duration flushingInterval = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	protected Duration idleTimeout;

	public LiveRedisItemReaderBuilder(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			AbstractRedisClient client, ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(client, valueReaderFactory);
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
	}

	public LiveRedisItemReaderBuilder<T, R> notificationQueueCapacity(int notificationQueueCapacity) {
		this.notificationQueueCapacity = notificationQueueCapacity;
		return this;
	}

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

	public LiveRedisItemReaderBuilder<T, R> keyPatterns(String... keyPatterns) {
		this.keyPatterns = keyPatterns;
		return this;
	}

	public static List<String> pubSubPatterns(int database, String... keyPatterns) {
		List<String> patterns = new ArrayList<>();
		for (String keyPattern : keyPatterns) {
			patterns.add(pubSubPattern(database, keyPattern));
		}
		return patterns;
	}

	public static String pubSubPattern(int database, String keyPattern) {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

	public PubSubSubscriber<String> pubSubSubscriber() {
		List<String> patterns = pubSubPatterns(database, keyPatterns);
		log.info("Creating keyspace notification reader with queue capacity {}", notificationQueueCapacity);
		if (client instanceof RedisClusterClient) {
			return new RedisClusterPubSubSubscriber<>(() -> ((RedisClusterClient) client).connectPubSub(codec),
					patterns);
		}
		return new RedisPubSubSubscriber<>(() -> ((RedisClient) client).connectPubSub(codec), patterns);
	}

	public LiveKeyItemReader<String> keyReader() {
		return new LiveKeyItemReader<>(pubSubSubscriber(), queue(notificationQueueCapacity), queuePollTimeout,
				DEFAULT_KEY_EXTRACTOR);
	}

	public LiveRedisItemReader<String, T> build() {
		return new LiveRedisItemReader<>(jobRepository, transactionManager, keyReader(), valueReader(), threads,
				chunkSize, valueQueue(), queuePollTimeout, skipPolicy, flushingInterval, idleTimeout);
	}
}