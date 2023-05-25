package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.FilteringItemProcessor;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.QueueItemWriter;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.common.SimpleStepRunner;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.DataStructureCodecReadOperation;
import com.redis.spring.batch.reader.DataStructureStringReadOperation;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparisonReadOperation;
import com.redis.spring.batch.reader.KeyDumpReadOperation;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ReaderOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.ScanOptions;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V, T> extends AbstractItemStreamItemReader<T> implements PollableItemReader<T> {

	public static final Duration DEFAULT_RUNNING_TIMEOUT = Duration.ofSeconds(5);
	public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

	private final JobRunner jobRunner;
	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final ItemReader<K> reader;
	private final ItemProcessor<K, K> processor;
	private final BatchOperation<K, V, K, T> operation;
	private final ReaderOptions options;

	private String name;
	private BlockingQueue<T> queue;
	private SimpleStepRunner<K, K> stepRunner;

	public RedisItemReader(JobRunner jobRunner, AbstractRedisClient client, RedisCodec<K, V> codec,
			ItemReader<K> reader, ItemProcessor<K, K> processor, BatchOperation<K, V, K, T> operation,
			ReaderOptions options) {
		setName(ClassUtils.getShortName(getClass()));
		this.jobRunner = jobRunner;
		this.client = client;
		this.codec = codec;
		this.reader = reader;
		this.processor = processor;
		this.operation = operation;
		this.options = options;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (stepRunner == null) {
			queue = queue();
			QueueItemWriter<T> queueWriter = new QueueItemWriter<>(queue);
			ProcessingItemWriter<K, T> writer = new ProcessingItemWriter<>(operationProcessor(), queueWriter);
			stepRunner = new SimpleStepRunner<>(jobRunner, reader, processor, writer, options.getStepOptions());
			stepRunner.setName(name);
			stepRunner.open(executionContext);
		}
	}

	private OperationItemStreamSupport<K, V, K, T> operationProcessor() {
		return new OperationItemStreamSupport<>(client, codec, options.getPoolOptions(), operation);
	}

	private BlockingQueue<T> queue() {
		BlockingQueue<T> blockingQueue = new LinkedBlockingQueue<>(options.getQueueOptions().getCapacity());
		Utils.createGaugeCollectionSize("reader.queue.size", blockingQueue);
		return blockingQueue;
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		if (stepRunner != null) {
			stepRunner.update(executionContext);
		}
	}

	@Override
	public synchronized void close() {
		if (stepRunner != null) {
			stepRunner.close();
			stepRunner = null;
		}
		queue = null;
		super.close();
	}

	@Override
	public synchronized T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		T item;
		do {
			item = queue.poll(options.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && stepRunner.isRunning());
		if (stepRunner.isJobFailed()) {
			throw new ItemStreamException("Reader job failed");
		}
		return item;
	}

	public synchronized List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		queue.drainTo(items, maxElements);
		return items;
	}

	public abstract static class AbstractBuilder<K, V, T, B extends AbstractBuilder<K, V, T, B>> {

		protected final AbstractRedisClient client;
		protected final RedisCodec<K, V> codec;
		private Optional<JobRunner> jobRunner = Optional.empty();
		protected ReaderOptions options = ReaderOptions.builder().build();

		protected AbstractBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		@SuppressWarnings("unchecked")
		public B jobRunner(JobRunner jobRunner) {
			this.jobRunner = Optional.of(jobRunner);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		protected B jobRunner(Optional<JobRunner> jobRunner) {
			this.jobRunner = jobRunner;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B options(ReaderOptions options) {
			this.options = options;
			return (B) this;
		}

		protected <B1 extends AbstractBuilder<?, ?, ?, ?>> B1 toBuilder(B1 builder) {
			builder.jobRunner(jobRunner);
			builder.options(options);
			return builder;
		}

		private JobRunner jobRunner() {
			return jobRunner.orElseGet(JobRunner::getInMemoryInstance);
		}

		public RedisItemReader<K, V, T> build() {
			return new RedisItemReader<>(jobRunner(), client, codec, keyReader(), keyProcessor(), operation(), options);
		}

		protected abstract BatchOperation<K, V, K, T> operation();

		protected ItemProcessor<K, K> keyProcessor() {
			return null;
		}

		protected abstract ItemReader<K> keyReader();

	}

	public static ComparatorBuilder compare(AbstractRedisClient left, AbstractRedisClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static ScanBuilder<String, String, DataStructure<String>> dataStructure(RedisModulesClusterClient client) {
		return new ScanBuilder<>(client, StringCodec.UTF8, new DataStructureStringReadOperation(client));
	}

	public static ScanBuilder<String, String, DataStructure<String>> dataStructure(RedisModulesClient client) {
		return new ScanBuilder<>(client, StringCodec.UTF8, new DataStructureStringReadOperation(client));
	}

	public static <K, V> ScanBuilder<K, V, DataStructure<K>> dataStructure(RedisModulesClient client,
			RedisCodec<K, V> codec) {
		return new ScanBuilder<>(client, codec, new DataStructureCodecReadOperation<>(client, codec));
	}

	public static <K, V> ScanBuilder<K, V, DataStructure<K>> dataStructure(RedisModulesClusterClient client,
			RedisCodec<K, V> codec) {
		return new ScanBuilder<>(client, codec, new DataStructureCodecReadOperation<>(client, codec));
	}

	public static ScanBuilder<byte[], byte[], KeyDump<byte[]>> keyDump(RedisModulesClient client) {
		return new ScanBuilder<>(client, ByteArrayCodec.INSTANCE, new KeyDumpReadOperation(client));
	}

	public static ScanBuilder<byte[], byte[], KeyDump<byte[]>> keyDump(RedisModulesClusterClient client) {
		return new ScanBuilder<>(client, ByteArrayCodec.INSTANCE, new KeyDumpReadOperation(client));
	}

	public static class ScanBuilder<K, V, T> extends AbstractBuilder<K, V, T, ScanBuilder<K, V, T>> {

		private final Operation<K, V, K, T> operation;
		private ScanOptions scanOptions = ScanOptions.builder().build();

		public ScanBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
			super(client, codec);
			this.operation = operation;
		}

		@Override
		protected BatchOperation<K, V, K, T> operation() {
			return new SimpleBatchOperation<>(operation);
		}

		public ScanBuilder<K, V, T> scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return this;
		}

		public LiveBuilder<K, V, T> live() {
			return toBuilder(new LiveBuilder<>(client, codec, operation)).keyPatterns(scanOptions.getMatch());
		}

		@Override
		protected ItemReader<K> keyReader() {
			return new ScanKeyItemReader<>(client, codec, scanOptions);
		}

	}

	public static class LiveBuilder<K, V, T> extends AbstractBuilder<K, V, T, LiveBuilder<K, V, T>> {

		public static final Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);
		public static final int DEFAULT_DATABASE = 0;
		protected static final String[] DEFAULT_KEY_PATTERNS = { ScanOptions.DEFAULT_MATCH };
		public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

		private final Operation<K, V, K, T> operation;
		private int database = DEFAULT_DATABASE;
		private String[] keyPatterns = DEFAULT_KEY_PATTERNS;
		private QueueOptions eventQueueOptions = QueueOptions.builder().build();
		private Predicate<K> keyFilter;

		public LiveBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
			super(client, codec);
			this.operation = operation;
			options.getStepOptions().setFlushingInterval(DEFAULT_FLUSHING_INTERVAL);
		}

		@Override
		protected BatchOperation<K, V, K, T> operation() {
			return new SimpleBatchOperation<>(operation);
		}

		public LiveBuilder<K, V, T> keyFilter(Predicate<K> filter) {
			this.keyFilter = filter;
			return this;
		}

		@Override
		protected ItemProcessor<K, K> keyProcessor() {
			if (keyFilter == null) {
				return null;
			}
			return new FilteringItemProcessor<>(keyFilter);
		}

		public static String[] defaultKeyPatterns() {
			return DEFAULT_KEY_PATTERNS;
		}

		public static String[] patterns(int database, String... keyPatterns) {
			return Stream.of(keyPatterns).map(p -> String.format(PUBSUB_PATTERN_FORMAT, database, p))
					.collect(Collectors.toList()).toArray(new String[0]);
		}

		@Override
		protected KeyspaceNotificationItemReader<K, V> keyReader() {
			return new KeyspaceNotificationItemReader<>(client, codec, eventQueueOptions,
					patterns(database, keyPatterns));
		}

		public LiveBuilder<K, V, T> database(int database) {
			this.database = database;
			return this;
		}

		public LiveBuilder<K, V, T> keyPatterns(String... patterns) {
			this.keyPatterns = patterns;
			return this;
		}

		public LiveBuilder<K, V, T> eventQueueOptions(QueueOptions options) {
			this.eventQueueOptions = options;
			return this;
		}

		public static String[] defaultNotificationPatterns() {
			return patterns(0, defaultKeyPatterns());
		}

	}

	public static class ComparatorBuilder extends AbstractBuilder<String, String, KeyComparison, ComparatorBuilder> {

		public static final Duration DEFAULT_TTL_TOLERANCE = Duration.ofMillis(100);

		private final AbstractRedisClient right;
		private Duration ttlTolerance = DEFAULT_TTL_TOLERANCE;
		private ScanOptions scanOptions = ScanOptions.builder().build();
		private PoolOptions rightPoolOptions = PoolOptions.builder().build();

		public ComparatorBuilder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left, StringCodec.UTF8);
			this.right = right;
		}

		public ComparatorBuilder ttlTolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public ComparatorBuilder scanOptions(ScanOptions options) {
			this.scanOptions = options;
			return this;
		}

		public ComparatorBuilder rightPoolOptions(PoolOptions options) {
			this.rightPoolOptions = options;
			return this;
		}

		@Override
		protected ItemReader<String> keyReader() {
			return new ScanKeyItemReader<>(client, codec, scanOptions);
		}

		@Override
		protected BatchOperation<String, String, String, KeyComparison> operation() {
			return new KeyComparisonReadOperation(client, right, rightPoolOptions, ttlTolerance);
		}

	}

}
