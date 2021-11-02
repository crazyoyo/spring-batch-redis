package com.redis.spring.batch.builder;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.transaction.PlatformTransactionManager;

import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.support.FlushingStepBuilder;
import com.redis.spring.batch.support.KeyValue;
import com.redis.spring.batch.support.LiveKeyItemReader;
import com.redis.spring.batch.support.LiveRedisItemReader;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class LiveRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>>
		extends RedisItemReaderBuilder<T, R, LiveRedisItemReaderBuilder<T, R>> {

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private List<String> keyPatterns = Arrays.asList(LiveKeyItemReaderBuilder.DEFAULT_KEY_PATTERN);
	private int database = LiveKeyItemReaderBuilder.DEFAULT_DATABASE;
	private int notificationQueueCapacity = LiveKeyItemReader.DEFAULT_QUEUE_CAPACITY;
	private Duration flushingInterval = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL;
	private Duration idleTimeout;

	public LiveRedisItemReaderBuilder(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			AbstractRedisClient client, ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(client, valueReaderFactory);
		this.jobRepository = jobRepository;
		this.transactionManager = transactionManager;
	}

	public LiveRedisItemReaderBuilder<T, R> flushingInterval(Duration flushingInterval) {
		this.flushingInterval = flushingInterval;
		return this;
	}

	public LiveRedisItemReaderBuilder<T, R> idleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
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
		return keyReaderBuilder().database(database).keyPatterns(keyPatterns).queueCapacity(notificationQueueCapacity)
				.build();
	}

	private LiveKeyItemReaderBuilder keyReaderBuilder() {
		if (client instanceof RedisClusterClient) {
			return LiveKeyItemReader.client((RedisClusterClient) client);
		}
		return LiveKeyItemReader.client((RedisClient) client);
	}
}