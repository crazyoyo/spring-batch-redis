package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemProcessor;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.support.job.JobFactory;

import io.lettuce.core.AbstractRedisClient;

@SuppressWarnings("unchecked")
public class LiveRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>>
		extends RedisItemReaderBuilder<T, R, LiveRedisItemReaderBuilder<T, R>> {

	public static final int DEFAULT_QUEUE_CAPACITY = 1000;
	public static final int DEFAULT_DATABASE = 0;
	public static final String DEFAULT_KEY_PATTERN = "*";
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	public static final List<String> DEFAULT_PUBSUB_PATTERNS = pubSubPatterns(DEFAULT_DATABASE, DEFAULT_KEY_PATTERN);

	protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private String[] keyPatterns = new String[] { DEFAULT_KEY_PATTERN };
	private int database = DEFAULT_DATABASE;
	protected Duration flushingInterval = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	protected Duration idleTimeout;

	public LiveRedisItemReaderBuilder(JobFactory jobFactory, AbstractRedisClient client,
			ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(jobFactory, client, valueReaderFactory);
	}

	public LiveRedisItemReaderBuilder<T, R> queueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
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

	@SuppressWarnings("rawtypes")
	public PollableItemReader<String> keyReader() {
		if (client instanceof RedisModulesClusterClient) {
			return new ClusterKeyspaceNotificationItemReader((Supplier) pubSubConnectionSupplier(),
					pubSubPatterns(database, keyPatterns), queueCapacity);
		}
		return new KeyspaceNotificationItemReader(pubSubConnectionSupplier(), pubSubPatterns(database, keyPatterns),
				queueCapacity);
	}

	public LiveRedisItemReader<String, T> build() {
		return new LiveRedisItemReader<>(jobFactory, keyReader(), valueReader(), threads, chunkSize, queue(),
				queuePollTimeout, skipPolicy, flushingInterval, idleTimeout);
	}
}