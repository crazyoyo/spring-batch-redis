package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.QueueItemWriter;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.common.SynchronizedPollableItemReader;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.DataStructureReadOperation;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparisonReadOperation;
import com.redis.spring.batch.reader.KeyDumpReadOperation;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationOrderingStrategy;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.StringDataStructureReadOperation;
import com.redis.spring.batch.step.FlushingChunkProvider;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V, T> extends AbstractItemStreamItemReader<T> implements PollableItemReader<T> {

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final ItemReader<K> reader;
	private ItemProcessor<K, K> processor;
	private final BatchOperation<K, V, K, T> operation;

	private JobRepository jobRepository;
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private PoolOptions poolOptions = PoolOptions.builder().build();
	private QueueOptions queueOptions = QueueOptions.builder().build();

	private String name;
	private JobExecution jobExecution;
	private BlockingQueue<T> queue;

	public RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, ItemReader<K> reader,
			BatchOperation<K, V, K, T> operation) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.reader = reader;
		this.operation = operation;
	}

	public RedisItemReader<K, V, T> withKeyProcessor(ItemProcessor<K, K> processor) {
		this.processor = processor;
		return this;
	}

	public RedisItemReader<K, V, T> withThreads(int threads) {
		this.threads = threads;
		return this;
	}

	public RedisItemReader<K, V, T> withChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
		return this;
	}

	public RedisItemReader<K, V, T> withPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
		return this;
	}

	public RedisItemReader<K, V, T> withQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
		return this;
	}

	public RedisItemReader<K, V, T> withJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
		return this;
	}

	@Override
	public void setName(String name) {
		super.setName(name);
		this.name = name;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (jobExecution != null) {
			return;
		}
		queue = queue();
		JobRepository repository;
		try {
			repository = jobRepository();
		} catch (Exception e) {
			throw new ItemStreamException("Could not initialize job repository", e);
		}
		StepBuilder stepBuilder = new StepBuilder(name + "-step");
		stepBuilder.repository(repository);
		stepBuilder.transactionManager(transactionManager());
		SimpleStepBuilder<K, K> step = step(stepBuilder);
		step.reader(reader());
		step.processor(processor);
		step.writer(writer());
		Utils.multiThread(step, threads);
		JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(repository);
		Job job = jobBuilderFactory.get(name).start(step.build()).build();
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(repository);
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		try {
			jobExecution = jobLauncher.run(job, new JobParameters());
		} catch (JobExecutionException e) {
			throw new ItemStreamException("Job execution failed", e);
		}
		while (!jobExecution.isRunning() && !jobExecution.getStatus().isUnsuccessful()
				&& jobExecution.getStatus() != BatchStatus.COMPLETED) {
			sleep();
		}
		sleep();
		if (jobExecution.getStatus().isUnsuccessful()) {
			throw new ItemStreamException("Could not run job",
					jobExecution.getAllFailureExceptions().iterator().next());
		}
	}

	private void sleep() {
		try {
			Thread.sleep(queueOptions.getPollTimeout().toMillis());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ItemStreamException("Interrupted during initialization", e);
		}
	}

	private PlatformTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

	public JobExecution getJobExecution() {
		return jobExecution;
	}

	private JobRepository jobRepository() throws Exception {
		if (jobRepository == null) {
			jobRepository = Utils.inMemoryJobRepository();
		}
		return jobRepository;
	}

	protected SimpleStepBuilder<K, K> step(StepBuilder step) {
		return step.chunk(chunkSize);
	}

	private ItemWriter<? super K> writer() {
		OperationItemStreamSupport<K, V, K, T> operationProcessor = new OperationItemStreamSupport<>(client, codec,
				operation);
		operationProcessor.withPoolOptions(poolOptions);
		QueueItemWriter<T> queueWriter = new QueueItemWriter<>(queue);
		return new ProcessingItemWriter<>(operationProcessor, queueWriter);
	}

	protected ItemReader<K> reader() {
		if (threads > 1) {
			if (reader instanceof PollableItemReader) {
				return new SynchronizedPollableItemReader<>((PollableItemReader<K>) reader);
			}
			if (reader instanceof ItemStreamReader) {
				SynchronizedItemStreamReader<K> synchronizedReader = new SynchronizedItemStreamReader<>();
				synchronizedReader.setDelegate((ItemStreamReader<K>) reader);
				return synchronizedReader;
			}
		}
		return reader;
	}

	private BlockingQueue<T> queue() {
		BlockingQueue<T> blockingQueue = new LinkedBlockingQueue<>(queueOptions.getCapacity());
		Utils.createGaugeCollectionSize("reader.queue.size", blockingQueue);
		return blockingQueue;
	}

	@Override
	public synchronized void close() {
		if (jobExecution != null) {
			if (jobExecution.isRunning()) {
				for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
					stepExecution.setTerminateOnly();
				}
				jobExecution.setStatus(BatchStatus.STOPPING);
			}
			jobExecution = null;
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
			item = queue.poll(queueOptions.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution != null && jobExecution.isRunning());
		if (jobExecution != null && jobExecution.getStatus().isUnsuccessful()) {
			throw new ItemStreamException("Reader job failed");
		}
		return item;
	}

	public synchronized List<T> read(int maxElements) {
		List<T> items = new ArrayList<>(maxElements);
		queue.drainTo(items, maxElements);
		return items;
	}

	public static <K, V> ScanReaderBuilder<K, V> client(RedisModulesClient client, RedisCodec<K, V> codec) {
		return new ScanReaderBuilder<>(client, codec);
	}

	public static ScanReaderBuilder<String, String> client(RedisModulesClient client) {
		return new ScanReaderBuilder<>(client, StringCodec.UTF8);
	}

	public static <K, V> ScanReaderBuilder<K, V> client(RedisClient client, RedisCodec<K, V> codec) {
		return new ScanReaderBuilder<>(client, codec);
	}

	public static ScanReaderBuilder<String, String> client(RedisClient client) {
		return new ScanReaderBuilder<>(client, StringCodec.UTF8);
	}

	public static ComparatorBuilder compare(RedisClient left, RedisClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static ComparatorBuilder compare(RedisModulesClient left, RedisClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static ComparatorBuilder compare(RedisClient left, RedisModulesClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static ComparatorBuilder compare(RedisModulesClient left, RedisModulesClient right) {
		return new ComparatorBuilder(left, right);
	}

	public abstract static class AbstractReaderBuilder<K, V, B extends AbstractReaderBuilder<K, V, B>>
			extends BaseBuilder<B> {

		protected final AbstractRedisClient client;
		protected final RedisCodec<K, V> codec;

		protected AbstractReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public RedisItemReader<K, V, KeyDump<K>> keyDump() {
			return reader(new KeyDumpReadOperation<>(client));
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public RedisItemReader<K, V, DataStructure<K>> dataStructure() {
			if (codec instanceof StringCodec) {
				return reader((Operation) new StringDataStructureReadOperation(client));
			}
			return reader(new DataStructureReadOperation<>(client, codec));
		}

		protected <T> RedisItemReader<K, V, T> reader(Operation<K, V, K, T> operation) {
			return configure(reader(new SimpleBatchOperation<>(operation)));
		}

		protected abstract <T> RedisItemReader<K, V, T> reader(BatchOperation<K, V, K, T> operation);

	}

	public static class BaseBuilder<B extends BaseBuilder<B>> {

		private JobRepository jobRepository;
		private int threads = DEFAULT_THREADS;
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private PoolOptions poolOptions = PoolOptions.builder().build();
		private QueueOptions queueOptions = QueueOptions.builder().build();

		@SuppressWarnings("unchecked")
		public B jobRepository(JobRepository jobRepository) {
			this.jobRepository = jobRepository;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B queueOptions(QueueOptions options) {
			this.queueOptions = options;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B poolOptions(PoolOptions options) {
			this.poolOptions = options;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B threads(int threads) {
			this.threads = threads;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return (B) this;
		}

		public <B1 extends BaseBuilder<B1>> B1 toBuilder(B1 builder) {
			builder.jobRepository(jobRepository);
			builder.chunkSize(chunkSize);
			builder.threads(threads);
			builder.poolOptions(poolOptions);
			builder.queueOptions(queueOptions);
			return builder;
		}

		protected <K, V, T, R extends RedisItemReader<K, V, T>> R configure(R reader) {
			reader.withJobRepository(jobRepository);
			reader.withChunkSize(chunkSize);
			reader.withThreads(threads);
			reader.withPoolOptions(poolOptions);
			reader.withQueueOptions(queueOptions);
			return reader;
		}

	}

	public static class ScanReaderBuilder<K, V> extends AbstractReaderBuilder<K, V, ScanReaderBuilder<K, V>> {

		protected String scanMatch = ScanKeyItemReader.DEFAULT_MATCH;
		private long scanCount = ScanKeyItemReader.DEFAULT_COUNT;
		private Optional<String> scanType = Optional.empty();

		public ScanReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public ScanReaderBuilder<K, V> scanMatch(String match) {
			this.scanMatch = match;
			return this;
		}

		public ScanReaderBuilder<K, V> scanCount(long count) {
			this.scanCount = count;
			return this;
		}

		public ScanReaderBuilder<K, V> scanType(Optional<String> type) {
			this.scanType = type;
			return this;
		}

		public ScanReaderBuilder<K, V> type(String type) {
			return scanType(Optional.of(type));
		}

		@Override
		protected <T> RedisItemReader<K, V, T> reader(BatchOperation<K, V, K, T> operation) {
			ScanKeyItemReader<K, V> keyReader = new ScanKeyItemReader<>(client, codec);
			keyReader.withCount(scanCount);
			keyReader.withMatch(scanMatch);
			keyReader.withType(scanType);
			return new RedisItemReader<>(client, codec, keyReader, operation);
		}

		public LiveReaderBuilder<K, V> live() {
			LiveReaderBuilder<K, V> builder = toBuilder(new LiveReaderBuilder<>(client, codec));
			builder.keyPatterns(scanMatch);
			scanType.ifPresent(builder::keyTypes);
			return builder;
		}

	}

	public static class LiveReaderBuilder<K, V> extends AbstractReaderBuilder<K, V, LiveReaderBuilder<K, V>> {

		private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
		public static final int DEFAULT_DATABASE = 0;
		protected static final String[] DEFAULT_KEY_PATTERNS = { ScanKeyItemReader.DEFAULT_MATCH };

		private int database = DEFAULT_DATABASE;
		private String[] keyPatterns = DEFAULT_KEY_PATTERNS;
		private List<String> keyTypes = new ArrayList<>();
		private QueueOptions notificationQueueOptions = QueueOptions.builder().build();
		private KeyspaceNotificationOrderingStrategy notificationOrdering = KeyspaceNotificationItemReader.DEFAULT_ORDERING;
		private Duration flushingInterval = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;
		private Duration idleTimeout; // no idle stream detection by default

		public LiveReaderBuilder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			super(client, codec);
		}

		public LiveReaderBuilder<K, V> idleTimeout(Duration timeout) {
			this.idleTimeout = timeout;
			return this;
		}

		public LiveReaderBuilder<K, V> flushingInterval(Duration interval) {
			this.flushingInterval = interval;
			return this;
		}

		public LiveReaderBuilder<K, V> database(int database) {
			this.database = database;
			return this;
		}

		public LiveReaderBuilder<K, V> keyPatterns(String... patterns) {
			this.keyPatterns = patterns;
			return this;
		}

		public LiveReaderBuilder<K, V> keyTypes(String... types) {
			this.keyTypes = Arrays.asList(types);
			return this;
		}

		public LiveReaderBuilder<K, V> notificationQueueOptions(QueueOptions options) {
			this.notificationQueueOptions = options;
			return this;
		}

		public LiveReaderBuilder<K, V> notificationOrdering(KeyspaceNotificationOrderingStrategy strategy) {
			this.notificationOrdering = strategy;
			return this;
		}

		@Override
		protected <T> RedisItemReader<K, V, T> reader(BatchOperation<K, V, K, T> operation) {
			KeyspaceNotificationItemReader<K, V> keyReader = new KeyspaceNotificationItemReader<>(client, codec);
			keyReader.withPatterns(patterns(database, keyPatterns));
			keyReader.withTypes(keyTypes);
			keyReader.withOrderingStrategy(notificationOrdering);
			keyReader.withQueueOptions(notificationQueueOptions);
			LiveRedisItemReader<K, V, T> reader = new LiveRedisItemReader<>(client, codec, keyReader, operation);
			reader.withFlushingInterval(flushingInterval);
			reader.withIdleTimeout(idleTimeout);
			return reader;
		}

		public static List<String> defaultPatterns() {
			return patterns(DEFAULT_DATABASE, DEFAULT_KEY_PATTERNS);
		}

		public static List<String> patterns(int database, String... keyPatterns) {
			return Stream.of(keyPatterns).map(k -> String.format(PUBSUB_PATTERN_FORMAT, database, k))
					.collect(Collectors.toList());
		}

		@Override
		public LiveRedisItemReader<K, V, DataStructure<K>> dataStructure() {
			return (LiveRedisItemReader<K, V, DataStructure<K>>) super.dataStructure();
		}

		@Override
		public LiveRedisItemReader<K, V, KeyDump<K>> keyDump() {
			return (LiveRedisItemReader<K, V, KeyDump<K>>) super.keyDump();
		}

	}

	public static class ComparatorBuilder extends BaseBuilder<ComparatorBuilder> {

		private final AbstractRedisClient left;
		private final AbstractRedisClient right;
		private long scanCount = ScanKeyItemReader.DEFAULT_COUNT;
		protected String scanMatch = ScanKeyItemReader.DEFAULT_MATCH;
		private Optional<String> scanType = Optional.empty();
		private Duration ttlTolerance = KeyComparisonReadOperation.DEFAULT_TTL_TOLERANCE;
		private PoolOptions rightPoolOptions = PoolOptions.builder().build();

		public ComparatorBuilder(AbstractRedisClient left, AbstractRedisClient right) {
			this.left = left;
			this.right = right;
		}

		public ComparatorBuilder scanCount(long count) {
			this.scanCount = count;
			return this;
		}

		public ComparatorBuilder scanMatch(String match) {
			this.scanMatch = match;
			return this;
		}

		public ComparatorBuilder scanType(String type) {
			return scanType(Optional.of(type));
		}

		public ComparatorBuilder scanType(Optional<String> type) {
			this.scanType = type;
			return this;
		}

		public ComparatorBuilder ttlTolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public ComparatorBuilder rightPoolOptions(PoolOptions options) {
			this.rightPoolOptions = options;
			return this;
		}

		public RedisItemReader<String, String, KeyComparison> build() {
			ScanKeyItemReader<String, String> keyReader = new ScanKeyItemReader<>(left, StringCodec.UTF8);
			keyReader.withCount(scanCount);
			keyReader.withMatch(scanMatch);
			keyReader.withType(scanType);
			KeyComparisonReadOperation operation = new KeyComparisonReadOperation(left, right);
			operation.withPoolOptions(rightPoolOptions);
			operation.withTtlTolerance(ttlTolerance);
			return configure(new RedisItemReader<>(left, StringCodec.UTF8, keyReader, operation));
		}

	}

}
