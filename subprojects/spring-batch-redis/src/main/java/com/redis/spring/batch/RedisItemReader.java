package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.reader.AbstractValueReader.ValueReaderFactory;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.RedisValueEnqueuer;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.StreamItemReader;
import com.redis.spring.batch.reader.StreamItemReader.AckPolicy;
import com.redis.spring.batch.reader.ValueReader;
import com.redis.spring.batch.support.JobRunner;
import com.redis.spring.batch.support.RedisConnectionBuilder;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemReader<T> {

	private final Log log = LogFactory.getLog(getClass());

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);
	public static final int DEFAULT_SKIP_LIMIT = 3;
	public static final SkipPolicy DEFAULT_SKIP_POLICY = limitCheckingSkipPolicy(DEFAULT_SKIP_LIMIT);

	private final AtomicInteger runningThreads = new AtomicInteger();
	protected final ItemReader<K> keyReader;
	private final ValueReader<K, T> valueReader;
	protected final BlockingQueue<T> valueQueue;
	protected final RedisValueEnqueuer<K, T> enqueuer;
	private final Optional<JobRunner> jobRunner;

	private final int threads;
	private final int chunkSize;
	private final Duration queuePollTimeout;
	private final SkipPolicy skipPolicy;
	private JobExecution jobExecution;
	private String name;

	protected RedisItemReader(AbstractBuilder<K, ?, T, ?> builder) {
		setName(ClassUtils.getShortName(getClass()));
		this.chunkSize = builder.chunkSize;
		this.queuePollTimeout = builder.queuePollTimeout;
		this.skipPolicy = builder.skipPolicy;
		this.threads = builder.threads;
		this.keyReader = builder.keyReader();
		this.valueReader = builder.valueReader();
		this.valueQueue = new LinkedBlockingQueue<>(builder.valueQueueCapacity);
		this.enqueuer = new RedisValueEnqueuer<>(valueReader, valueQueue);
		this.jobRunner = builder.jobRunner;
	}

	private static SkipPolicy limitCheckingSkipPolicy(int skipLimit) {
		return new LimitCheckingItemSkipPolicy(skipLimit, Stream
				.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class)
				.collect(Collectors.toMap(t -> t, t -> true)));
	}

	public ValueReader<K, T> getValueReader() {
		return valueReader;
	}

	@Override
	public void setName(String name) {
		this.name = name;
		super.setName(name);
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		synchronized (runningThreads) {
			if (jobExecution == null) {
				doOpen();
			}
			runningThreads.incrementAndGet();
			super.open(executionContext);
		}
	}

	protected void doOpen() {
		JobRunner runner;
		if (jobRunner.isEmpty()) {
			try {
				runner = JobRunner.inMemory();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize job runner", e);
			}
		} else {
			runner = jobRunner.get();
		}
		Utils.createGaugeCollectionSize("reader.queue.size", valueQueue);
		SimpleStepBuilder<K, K> step = createStep(runner);
		FaultTolerantStepBuilder<K, K> stepBuilder = step.faultTolerant();
		stepBuilder.skipPolicy(skipPolicy);
		if (threads > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.afterPropertiesSet();
			stepBuilder.taskExecutor(taskExecutor).throttleLimit(threads);
		}
		SimpleJobBuilder simpleJobBuilder = runner.job(name).start(stepBuilder.build());
		Job job = simpleJobBuilder.build();
		try {
			jobExecution = runner.runAsync(job);
		} catch (JobExecutionException e) {
			throw new ItemStreamException(String.format("Could not run job %s", name), e);
		}
	}

	protected SimpleStepBuilder<K, K> createStep(JobRunner runner) {
		return runner.step(name, chunkSize, keyReader, enqueuer);
	}

	@Override
	public T read() throws Exception {
		T item;
		do {
			item = valueQueue.poll(queuePollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful());
		return item;
	}

	public List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		valueQueue.drainTo(items, maxElements);
		return items;
	}

	@Override
	public void close() {
		super.close();
		if (runningThreads.decrementAndGet() > 0) {
			return;
		}
		synchronized (runningThreads) {
			if (jobRunner.isPresent()) {
				jobRunner.get().awaitTermination(jobExecution);
			}
			if (!valueQueue.isEmpty()) {
				log.warn("Closing with items still in queue");
			}
			jobExecution = null;
		}
	}

	public boolean isOpen() {
		return jobExecution != null;
	}

	public static Builder<String, String, DataStructure<String>> dataStructure(AbstractRedisClient client) {
		return dataStructure(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V, DataStructure<K>> dataStructure(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return new Builder<>(client, codec, ValueReaderFactory.dataStructure());
	}

	public static Builder<String, String, KeyValue<String, byte[]>> keyDump(AbstractRedisClient client) {
		return keyDump(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V, KeyValue<K, byte[]>> keyDump(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return new Builder<>(client, codec, ValueReaderFactory.keyDump());
	}

	public static <K, V> StreamBuilder<K, V> stream(AbstractRedisClient client, RedisCodec<K, V> codec, K name) {
		return new StreamBuilder<>(client, codec, name);
	}

	public static StreamBuilder<String, String> stream(AbstractRedisClient client, String name) {
		return stream(client, StringCodec.UTF8, name);
	}

	public static class Builder<K, V, T extends KeyValue<K, ?>> extends AbstractBuilder<K, V, T, Builder<K, V, T>> {

		private String match = ScanKeyItemReader.DEFAULT_SCAN_MATCH;
		private long count = ScanKeyItemReader.DEFAULT_SCAN_COUNT;
		private Optional<String> type = Optional.empty();

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec,
				ValueReaderFactory<K, V, T> valueReaderFactory) {
			super(client, codec, valueReaderFactory);
		}

		public Builder<K, V, T> match(String match) {
			this.match = match;
			return this;
		}

		public Builder<K, V, T> count(long count) {
			this.count = count;
			return this;
		}

		public Builder<K, V, T> type(String type) {
			this.type = Optional.of(type);
			return this;
		}

		@Override
		protected ItemReader<K> keyReader() {
			return new ScanKeyItemReader<>(connectionSupplier(), sync(), match, count, type);
		}

		public RedisItemReader<K, T> build() {
			return new RedisItemReader<>(this);
		}

		public LiveRedisItemReader.Builder<K, V, T> live() {
			LiveRedisItemReader.Builder<K, V, T> live = new LiveRedisItemReader.Builder<>(client, codec,
					valueReaderFactory);
			live.keyPatterns(match);
			live.threads(threads);
			live.valueQueueCapacity(valueQueueCapacity);
			live.queuePollTimeout(queuePollTimeout);
			live.skipPolicy(skipPolicy);
			return live;
		}

	}

	public static class StreamBuilder<K, V> extends RedisConnectionBuilder<K, V, StreamBuilder<K, V>> {

		public static final String DEFAULT_CONSUMER_GROUP = ClassUtils.getShortName(StreamItemReader.class);
		public static final String DEFAULT_CONSUMER = "consumer1";

		private final K stream;
		private String offset = "0-0";
		private Duration block = StreamItemReader.DEFAULT_BLOCK;
		private long count = StreamItemReader.DEFAULT_COUNT;
		private K consumerGroup;
		private K consumer;
		private AckPolicy ackPolicy = StreamItemReader.DEFAULT_ACK_POLICY;

		public StreamBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, K stream) {
			super(client, codec);
			this.stream = stream;
		}

		public StreamBuilder<K, V> offset(String offset) {
			this.offset = offset;
			return this;
		}

		public StreamBuilder<K, V> block(Duration block) {
			this.block = block;
			return this;
		}

		public StreamBuilder<K, V> count(long count) {
			this.count = count;
			return this;
		}

		public StreamBuilder<K, V> consumerGroup(K consumerGroup) {
			this.consumerGroup = consumerGroup;
			return this;
		}

		public StreamBuilder<K, V> consumer(K consumer) {
			this.consumer = consumer;
			return this;
		}

		public StreamBuilder<K, V> ackPolicy(AckPolicy ackPolicy) {
			this.ackPolicy = ackPolicy;
			return this;
		}

		public StreamItemReader<K, V> build() {
			StreamItemReader<K, V> reader = new StreamItemReader<>(connectionSupplier(), poolConfig, sync(),
					StreamOffset.from(stream, offset));
			reader.setAckPolicy(ackPolicy);
			reader.setBlock(block);
			reader.setConsumer(consumer == null ? encodeKey(DEFAULT_CONSUMER) : consumer);
			reader.setConsumerGroup(consumerGroup == null ? encodeKey(DEFAULT_CONSUMER_GROUP) : consumerGroup);
			reader.setCount(count);
			return reader;
		}

	}

	public abstract static class AbstractBuilder<K, V, T extends KeyValue<K, ?>, B extends AbstractBuilder<K, V, T, B>>
			extends RedisConnectionBuilder<K, V, B> {

		protected final ValueReaderFactory<K, V, T> valueReaderFactory;
		protected int chunkSize = RedisItemReader.DEFAULT_CHUNK_SIZE;
		protected int threads = RedisItemReader.DEFAULT_THREADS;
		protected int valueQueueCapacity = RedisItemReader.DEFAULT_QUEUE_CAPACITY;
		protected Duration queuePollTimeout = RedisItemReader.DEFAULT_QUEUE_POLL_TIMEOUT;
		protected SkipPolicy skipPolicy = RedisItemReader.DEFAULT_SKIP_POLICY;
		protected Optional<JobRunner> jobRunner = Optional.empty();

		protected AbstractBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
				ValueReaderFactory<K, V, T> valueReaderFactory) {
			super(client, codec);
			this.valueReaderFactory = valueReaderFactory;
		}

		protected abstract ItemReader<K> keyReader();

		@SuppressWarnings("unchecked")
		public B jobRunner(JobRunner jobRunner) {
			this.jobRunner = Optional.of(jobRunner);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B threads(int threads) {
			this.threads = threads;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B valueQueueCapacity(int queueCapacity) {
			this.valueQueueCapacity = queueCapacity;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B queuePollTimeout(Duration queuePollTimeout) {
			this.queuePollTimeout = queuePollTimeout;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B skipPolicy(SkipPolicy skipPolicy) {
			this.skipPolicy = skipPolicy;
			return (B) this;
		}

		protected ValueReader<K, T> valueReader() {
			return valueReaderFactory.create(connectionSupplier(), poolConfig, async());
		}

	}

}
