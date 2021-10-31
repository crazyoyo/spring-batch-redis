package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

import com.redis.spring.batch.RedisItemReader;
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
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final Map<Class<? extends Throwable>, Boolean> DEFAULT_SKIPPABLE_EXCEPTIONS = Stream
			.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class)
			.collect(Collectors.toMap(t -> t, t -> true));
	public static final int DEFAULT_SKIP_LIMIT = 3;
	public static final SkipPolicy DEFAULT_SKIP_POLICY = new LimitCheckingItemSkipPolicy(DEFAULT_SKIP_LIMIT,
			DEFAULT_SKIPPABLE_EXCEPTIONS);

	protected final AbstractRedisClient client;
	protected final ValueReaderFactory<String, String, T, R> valueReaderFactory;

	protected int threads = DEFAULT_THREADS;
	protected int chunkSize = DEFAULT_CHUNK_SIZE;
	private int valueQueueCapacity = DEFAULT_QUEUE_CAPACITY;
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

	public B valueQueueCapacity(int capacity) {
		Utils.assertPositive(capacity, "Value queue capacity");
		this.valueQueueCapacity = capacity;
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

	protected <E> BlockingQueue<E> queue(int capacity) {
		return new LinkedBlockingQueue<>(capacity);
	}

	protected BlockingQueue<T> valueQueue() {
		return queue(valueQueueCapacity);
	}

	protected R valueReader() {
		return valueReaderFactory.create(connectionSupplier(), poolConfig, async());
	}

	public static class ScanRedisItemReaderBuilder<T extends KeyValue<String, ?>, R extends ItemProcessor<List<? extends String>, List<T>>>
			extends RedisItemReaderBuilder<T, R, ScanRedisItemReaderBuilder<T, R>> {

		public static final String DEFAULT_SCAN_MATCH = "*";
		public static final long DEFAULT_SCAN_COUNT = 1000;

		private final JobRepository jobRepository;
		private final PlatformTransactionManager transactionManager;
		private String scanMatch = DEFAULT_SCAN_MATCH;
		private long scanCount = DEFAULT_SCAN_COUNT;
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

		public ItemReader<String> keyReader() {
			return new ScanKeyItemReader<>(connectionSupplier(), sync(), scanMatch, scanCount, scanType);
		}

		public RedisItemReader<String, T> build() {
			return new RedisItemReader<>(jobRepository, transactionManager, keyReader(), valueReader(), threads,
					chunkSize, valueQueue(), queuePollTimeout, skipPolicy);
		}

		public LiveRedisItemReaderBuilder<T, R> live() {
			return new LiveRedisItemReaderBuilder<>(jobRepository, transactionManager, client, valueReaderFactory);
		}
	}

}