package com.redis.spring.batch.reader;

import java.time.Duration;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class LiveRedisItemReader<K, V, T> extends RedisItemReader<K, V, T> {

	private Duration flushingInterval = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;
	private Duration idleTimeout; // no idle stream detection by default

	public LiveRedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, PollableItemReader<K> reader,
			BatchOperation<K, V, K, T> operation) {
		super(client, codec, reader, operation);
	}

	public void setFlushingInterval(Duration flushingInterval) {
		this.flushingInterval = flushingInterval;
	}

	public void setIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	@Override
	protected SimpleStepBuilder<K, K> step(StepBuilder stepBuilder) {
		SimpleStepBuilder<K, K> step = super.step(stepBuilder);
		return new FlushingStepBuilder<>(step).flushingInterval(flushingInterval).idleTimeout(idleTimeout);
	}

}
