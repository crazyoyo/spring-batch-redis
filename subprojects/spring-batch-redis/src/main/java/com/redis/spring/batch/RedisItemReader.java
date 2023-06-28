package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.OperationItemStreamSupport;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.QueueItemWriter;
import com.redis.spring.batch.common.SimpleBatchOperation;
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
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
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
	private Optional<ReadFrom> readFrom = Optional.empty();
	private QueueOptions queueOptions = QueueOptions.builder().build();
	private String name;
	private JobExecution jobExecution;
	private BlockingQueue<T> queue;

	public RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, ItemReader<K> keyReader,
			BatchOperation<K, V, K, T> operation) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.reader = keyReader;
		this.operation = operation;
	}

	public void setKeyProcessor(ItemProcessor<K, K> processor) {
		this.processor = processor;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
	}

	public void setReadFrom(ReadFrom readFrom) {
		setReadFrom(Optional.of(readFrom));
	}

	public void setReadFrom(Optional<ReadFrom> readFrom) {
		this.readFrom = readFrom;
	}

	public void setQueueOptions(QueueOptions queueOptions) {
		this.queueOptions = queueOptions;
	}

	public void setJobRepository(JobRepository jobRepository) {
		this.jobRepository = jobRepository;
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
		step.reader(threads > 1 ? Utils.synchronizedReader(reader) : reader);
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
		operationProcessor.setPoolOptions(poolOptions);
		operationProcessor.setReadFrom(readFrom);
		QueueItemWriter<T> queueWriter = new QueueItemWriter<>(queue);
		return new ProcessingItemWriter<>(operationProcessor, queueWriter);
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

	public boolean isOpen() {
		return jobExecution != null;
	}

	@Override
	public synchronized T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public synchronized T read() throws Exception {
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

	public static ScanReaderBuilder client(RedisModulesClient client) {
		return new ScanReaderBuilder(client);
	}

	public static ScanReaderBuilder client(RedisModulesClusterClient client) {
		return new ScanReaderBuilder(client);
	}

	public static ComparatorBuilder compare(RedisModulesClient left, RedisModulesClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static ComparatorBuilder compare(RedisModulesClient left, RedisModulesClusterClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static ComparatorBuilder compare(RedisModulesClusterClient left, RedisModulesClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static ComparatorBuilder compare(RedisModulesClusterClient left, RedisModulesClusterClient right) {
		return new ComparatorBuilder(left, right);
	}

	public static class BaseReaderBuilder<B extends BaseReaderBuilder<B>> {

		protected final AbstractRedisClient client;
		private JobRepository jobRepository;
		private int threads = DEFAULT_THREADS;
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private PoolOptions poolOptions = PoolOptions.builder().build();
		protected Optional<ReadFrom> readFrom = Optional.empty();
		private QueueOptions queueOptions = QueueOptions.builder().build();

		protected BaseReaderBuilder(AbstractRedisClient client) {
			this.client = client;
		}

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

		public B readFrom(ReadFrom readFrom) {
			return readFrom(Optional.of(readFrom));
		}

		@SuppressWarnings("unchecked")
		public B readFrom(Optional<ReadFrom> readFrom) {
			this.readFrom = readFrom;
			return (B) this;
		}

		public <B1 extends BaseReaderBuilder<B1>> B1 toBuilder(B1 builder) {
			builder.jobRepository(jobRepository);
			builder.chunkSize(chunkSize);
			builder.threads(threads);
			builder.poolOptions(poolOptions);
			builder.queueOptions(queueOptions);
			builder.readFrom(readFrom);
			return builder;
		}

		protected void configure(RedisItemReader<?, ?, ?> reader) {
			reader.setJobRepository(jobRepository);
			reader.setChunkSize(chunkSize);
			reader.setThreads(threads);
			reader.setPoolOptions(poolOptions);
			reader.setQueueOptions(queueOptions);
			reader.setReadFrom(readFrom);
		}

	}

	public static class BaseScanReaderBuilder<B extends BaseScanReaderBuilder<B>> extends BaseReaderBuilder<B> {

		protected String scanMatch = ScanKeyItemReader.DEFAULT_MATCH;
		private long scanCount = ScanKeyItemReader.DEFAULT_COUNT;
		protected Optional<String> scanType = Optional.empty();

		protected BaseScanReaderBuilder(AbstractRedisClient client) {
			super(client);
		}

		@SuppressWarnings("unchecked")
		public B scanMatch(String match) {
			this.scanMatch = match;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B scanCount(long count) {
			this.scanCount = count;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B scanType(Optional<String> type) {
			this.scanType = type;
			return (B) this;
		}

		public B scanType(String type) {
			return scanType(Optional.of(type));
		}

		protected <K, V, T> RedisItemReader<K, V, T> reader(RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
			return reader(codec, SimpleBatchOperation.of(operation));
		}

		protected <K, V, T> RedisItemReader<K, V, T> reader(RedisCodec<K, V> codec,
				BatchOperation<K, V, K, T> operation) {
			Supplier<StatefulConnection<K, V>> connectionSupplier = Utils.connectionSupplier(client, codec, readFrom);
			ScanKeyItemReader<K, V> keyReader = new ScanKeyItemReader<>(connectionSupplier);
			keyReader.setCount(scanCount);
			keyReader.setMatch(scanMatch);
			keyReader.setType(scanType);
			RedisItemReader<K, V, T> reader = new RedisItemReader<>(client, codec, keyReader, operation);
			configure(reader);
			return reader;

		}

	}

	public static class ScanReaderBuilder extends BaseScanReaderBuilder<ScanReaderBuilder> {

		public ScanReaderBuilder(AbstractRedisClient client) {
			super(client);
		}

		public LiveReaderBuilder live() {
			LiveReaderBuilder builder = toBuilder(new LiveReaderBuilder(client));
			builder.keyPatterns(scanMatch);
			scanType.ifPresent(builder::keyTypes);
			return builder;
		}

		public RedisItemReader<byte[], byte[], KeyDump<byte[]>> keyDump() {
			return super.reader(ByteArrayCodec.INSTANCE, new KeyDumpReadOperation(client));
		}

		public RedisItemReader<String, String, DataStructure<String>> dataStructure() {
			return super.reader(StringCodec.UTF8, new StringDataStructureReadOperation(client));
		}

		public <K, V> RedisItemReader<K, V, DataStructure<K>> dataStructure(RedisCodec<K, V> codec) {
			return super.reader(codec, new DataStructureReadOperation<>(client, codec));
		}

	}

	public static class LiveReaderBuilder extends BaseReaderBuilder<LiveReaderBuilder> {

		public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
		public static final int DEFAULT_DATABASE = 0;
		private static final String[] DEFAULT_KEY_PATTERNS = { ScanKeyItemReader.DEFAULT_MATCH };

		private int database = DEFAULT_DATABASE;
		private String[] keyPatterns = DEFAULT_KEY_PATTERNS;
		private List<String> keyTypes = new ArrayList<>();
		private QueueOptions notificationQueueOptions = QueueOptions.builder().build();
		private KeyspaceNotificationOrderingStrategy notificationOrdering = KeyspaceNotificationItemReader.DEFAULT_ORDERING;
		private Duration flushingInterval = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;
		private Duration idleTimeout; // no idle stream detection by default

		public LiveReaderBuilder(AbstractRedisClient client) {
			super(client);
		}

		public LiveReaderBuilder idleTimeout(Duration timeout) {
			this.idleTimeout = timeout;
			return this;
		}

		public LiveReaderBuilder flushingInterval(Duration interval) {
			this.flushingInterval = interval;
			return this;
		}

		public LiveReaderBuilder database(int database) {
			this.database = database;
			return this;
		}

		public LiveReaderBuilder keyPatterns(String... patterns) {
			this.keyPatterns = patterns;
			return this;
		}

		public LiveReaderBuilder keyTypes(String... types) {
			this.keyTypes = Arrays.asList(types);
			return this;
		}

		public LiveReaderBuilder notificationQueueOptions(QueueOptions options) {
			this.notificationQueueOptions = options;
			return this;
		}

		public LiveReaderBuilder notificationOrdering(KeyspaceNotificationOrderingStrategy strategy) {
			this.notificationOrdering = strategy;
			return this;
		}

		private <K, V> KeyspaceNotificationItemReader<K, V> keyReader(RedisCodec<K, V> codec) {
			KeyspaceNotificationItemReader<K, V> keyReader = new KeyspaceNotificationItemReader<>(client, codec);
			keyReader.setPubSubPatterns(pubSubPatterns(database, keyPatterns));
			keyReader.setTypes(keyTypes);
			keyReader.setOrderingStrategy(notificationOrdering);
			keyReader.setQueueOptions(notificationQueueOptions);
			return keyReader;
		}

		public LiveRedisItemReader<byte[], byte[], KeyDump<byte[]>> keyDump() {
			return reader(ByteArrayCodec.INSTANCE, new KeyDumpReadOperation(client));
		}

		public LiveRedisItemReader<String, String, DataStructure<String>> dataStructure() {
			return reader(StringCodec.UTF8, new StringDataStructureReadOperation(client));
		}

		public <K, V> LiveRedisItemReader<K, V, DataStructure<K>> dataStructure(RedisCodec<K, V> codec) {
			return reader(codec, new DataStructureReadOperation<>(client, codec));
		}

		private <K, V, T> LiveRedisItemReader<K, V, T> reader(RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
			LiveRedisItemReader<K, V, T> reader = new LiveRedisItemReader<>(client, codec, keyReader(codec),
					SimpleBatchOperation.of(operation));
			reader.setFlushingInterval(flushingInterval);
			reader.setIdleTimeout(idleTimeout);
			configure(reader);
			return reader;
		}

		public static List<String> defaultPubSubPatterns() {
			return pubSubPatterns(DEFAULT_DATABASE, DEFAULT_KEY_PATTERNS);
		}

		public static List<String> pubSubPatterns(int database, String... keyPatterns) {
			return Stream.of(keyPatterns).map(k -> String.format(PUBSUB_PATTERN_FORMAT, database, k))
					.collect(Collectors.toList());
		}

	}

	public static class ComparatorBuilder extends BaseScanReaderBuilder<ComparatorBuilder> {

		private final AbstractRedisClient right;
		private Duration ttlTolerance = KeyComparisonReadOperation.DEFAULT_TTL_TOLERANCE;
		private PoolOptions rightPoolOptions = PoolOptions.builder().build();
		private Optional<ReadFrom> rightReadFrom = Optional.empty();

		public ComparatorBuilder(AbstractRedisClient left, AbstractRedisClient right) {
			super(left);
			this.right = right;
		}

		public ComparatorBuilder ttlTolerance(Duration ttlTolerance) {
			this.ttlTolerance = ttlTolerance;
			return this;
		}

		public ComparatorBuilder rightPoolOptions(PoolOptions options) {
			this.rightPoolOptions = options;
			return this;
		}

		public ComparatorBuilder rightReadFrom(Optional<ReadFrom> readFrom) {
			this.rightReadFrom = readFrom;
			return this;
		}

		public RedisItemReader<String, String, KeyComparison> build() {
			KeyComparisonReadOperation operation = new KeyComparisonReadOperation(client, right);
			operation.setPoolOptions(rightPoolOptions);
			operation.setTtlTolerance(ttlTolerance);
			operation.setReadFrom(rightReadFrom);
			return super.reader(StringCodec.UTF8, operation);
		}

	}

}
