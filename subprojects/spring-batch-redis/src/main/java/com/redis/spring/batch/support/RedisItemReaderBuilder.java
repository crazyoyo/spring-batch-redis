package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;

import com.redis.spring.batch.support.AbstractValueReader.ValueReaderFactory;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.codec.StringCodec;

@SuppressWarnings("unchecked")
public class RedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>, B extends RedisItemReaderBuilder<T, R, B>>
		extends CommandBuilder<String, String, B> {

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_QUEUE_CAPACITY = 1000;
	public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final Map<Class<? extends Throwable>, Boolean> DEFAULT_SKIPPABLE_EXCEPTIONS = Stream
			.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class)
			.collect(Collectors.toMap(t -> t, t -> true));
	public static final int DEFAULT_SKIP_LIMIT = 3;
	public static final SkipPolicy DEFAULT_SKIP_POLICY = new LimitCheckingItemSkipPolicy(DEFAULT_SKIP_LIMIT,
			DEFAULT_SKIPPABLE_EXCEPTIONS);

	protected final ValueReaderFactory<String, String, T, R> valueReaderFactory;
	protected final AbstractRedisClient client;

	protected int threads = DEFAULT_THREADS;
	protected int chunkSize = DEFAULT_CHUNK_SIZE;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	protected Duration queuePollTimeout = DEFAULT_QUEUE_POLL_TIMEOUT;
	protected SkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

	public RedisItemReaderBuilder(AbstractRedisClient client,
			ValueReaderFactory<String, String, T, R> valueReaderFactory) {
		super(client, StringCodec.UTF8);
		this.client = client;
		this.valueReaderFactory = valueReaderFactory;
	}

	public B threads(int threads) {
		Utils.assertPositive(threads, "Thread count");
		this.threads = threads;
		return (B) this;
	}

	public B chunkSize(int chunkSize) {
		Utils.assertPositive(chunkSize, "Chunk size");
		this.chunkSize = chunkSize;
		return (B) this;
	}

	public B queueCapacity(int queueCapacity) {
		Utils.assertPositive(queueCapacity, "Queue capacity");
		this.queueCapacity = queueCapacity;
		return (B) this;
	}

	public B queuePollTimeout(Duration queuePollTimeout) {
		Utils.assertPositive(queuePollTimeout, "Queue poll timeout");
		this.queuePollTimeout = queuePollTimeout;
		return (B) this;
	}

	public B skipPolicy(SkipPolicy skipPolicy) {
		Assert.notNull(skipPolicy, "Skip policy must not be null");
		this.skipPolicy = skipPolicy;
		return (B) this;
	}

	protected BlockingQueue<T> queue() {
		return new LinkedBlockingDeque<>(queueCapacity);
	}

	protected R valueReader() {
		return valueReaderFactory.create(connectionSupplier(), poolConfig, async());
	}

}