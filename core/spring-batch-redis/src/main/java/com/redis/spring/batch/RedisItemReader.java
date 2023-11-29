package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyItemReader;
import com.redis.spring.batch.reader.KeyScanItemReader;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.ProcessingItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.Metrics;

public abstract class RedisItemReader<K, V, T> implements ItemStreamReader<T>, PollableItemReader<T> {

	private final Log log = LogFactory.getLog(getClass());

	public enum ReaderMode {
		SCAN, LIVE
	}

	public static final String QUEUE_METER = "redis.batch.reader.queue.size";

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	public static final int DEFAULT_THREADS = 1;

	public static final int DEFAULT_CHUNK_SIZE = 50;

	public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(10);

	public static final OrderingStrategy DEFAULT_ORDERING = OrderingStrategy.PRIORITY;

	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;

	public static final ReaderMode DEFAULT_MODE = ReaderMode.SCAN;

	public static final int DEFAULT_SKIP_LIMIT = 0;

	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

	private static final Duration DEFAULT_OPEN_TIMEOUT = Duration.ofSeconds(3);

	private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(3);

	private final AbstractRedisClient client;

	private final RedisCodec<K, V> codec;

	private ReaderMode mode = DEFAULT_MODE;

	private int skipLimit = DEFAULT_SKIP_LIMIT;

	private int retryLimit = DEFAULT_RETRY_LIMIT;

	private List<Class<? extends Throwable>> skippableExceptions = defaultNonRetriableExceptions();

	private List<Class<? extends Throwable>> nonSkippableExceptions = defaultRetriableExceptions();

	private List<Class<? extends Throwable>> retriableExceptions = defaultRetriableExceptions();

	private List<Class<? extends Throwable>> nonRetriableExceptions = defaultNonRetriableExceptions();

	private int database;

	private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

	private Duration idleTimeout;

	private long scanCount;

	private ItemProcessor<K, K> keyProcessor;

	private ReadFrom readFrom;

	private int threads = DEFAULT_THREADS;

	private int chunkSize = DEFAULT_CHUNK_SIZE;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private String keyPattern;

	private DataType keyType;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	private Duration openTimeout = DEFAULT_OPEN_TIMEOUT;

	private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

	private JobRepository jobRepository;

	private PlatformTransactionManager transactionManager;

	private TaskExecutorJobLauncher jobLauncher;

	private String name;

	private JobExecution jobExecution;

	private KeyItemReader<K> keyReader;

	private ProcessingItemWriter<K, T> writer;

	protected RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public RedisCodec<K, V> getCodec() {
		return codec;
	}

	public ReaderMode getMode() {
		return mode;
	}

	public void addSkippableException(Class<? extends Throwable> exception) {
		skippableExceptions.add(exception);
	}

	public void addNonSkippableException(Class<? extends Throwable> exception) {
		nonSkippableExceptions.add(exception);
	}

	public void addRetriableException(Class<? extends Throwable> exception) {
		retriableExceptions.add(exception);
	}

	public void addNonRetriableException(Class<? extends Throwable> exception) {
		nonRetriableExceptions.add(exception);
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public void setCloseTimeout(Duration closeTimeout) {
		this.closeTimeout = closeTimeout;
	}

	public void setOpenTimeout(Duration timeout) {
		this.openTimeout = timeout;
	}

	public void setJobRepository(JobRepository repository) {
		this.jobRepository = repository;
	}

	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public ItemProcessor<K, K> getKeyProcessor() {
		return keyProcessor;
	}

	public void setKeyProcessor(ItemProcessor<K, K> processor) {
		this.keyProcessor = processor;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public void setChunkSize(int size) {
		this.chunkSize = size;
	}

	public void setQueueCapacity(int capacity) {
		this.queueCapacity = capacity;
	}

	public void setMode(ReaderMode mode) {
		this.mode = mode;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public void setKeyPattern(String globPattern) {
		this.keyPattern = globPattern;
	}

	public void setKeyType(DataType type) {
		this.keyType = type;
	}

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

	public void setIdleTimeout(Duration timeout) {
		this.idleTimeout = timeout;
	}

	public List<Class<? extends Throwable>> getRetriableExceptions() {
		return retriableExceptions;
	}

	public void setRetriableExceptions(List<Class<? extends Throwable>> retriableExceptions) {
		this.retriableExceptions = retriableExceptions;
	}

	public List<Class<? extends Throwable>> getNonRetriableExceptions() {
		return nonRetriableExceptions;
	}

	public void setNonRetriableExceptions(List<Class<? extends Throwable>> nonRetriableExceptions) {
		this.nonRetriableExceptions = nonRetriableExceptions;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public int getDatabase() {
		return database;
	}

	public OrderingStrategy getOrderingStrategy() {
		return orderingStrategy;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public long getScanCount() {
		return scanCount;
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public int getThreads() {
		return threads;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public DataType getKeyType() {
		return keyType;
	}

	public Duration getPollTimeout() {
		return pollTimeout;
	}

	public Duration getOpenTimeout() {
		return openTimeout;
	}

	public void setFlushInterval(Duration interval) {
		this.flushInterval = interval;
	}

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public void setOrderingStrategy(OrderingStrategy strategy) {
		this.orderingStrategy = strategy;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ItemReader<K> getKeyReader() {
		return keyReader;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (!isOpen()) {
			log.debug(String.format("Opening %s", name));
			try {
				initializeJobInfrastructure();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize job infrastructure", e);
			}
			Job job = new JobBuilder(name, jobRepository).start(step().build()).build();
			jobExecution = execute(job);
			log.debug(String.format("Opened %s", name));
		}
	}

	private JobExecution execute(Job job) {
		JobExecution execution;
		try {
			execution = jobLauncher.run(job, new JobParameters());
		} catch (JobExecutionException e) {
			throw new ItemStreamException("Job execution failed", e);
		}
		Await await = new Await();
		boolean executed;
		try {
			executed = await.await(() -> isRunning(execution) || isCompleted(execution), openTimeout);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ItemStreamException("Interruped while waiting for job to start", e);
		}
		if (!executed) {
			throw new ItemStreamException("Timeout waiting for job to run");
		}
		if (execution.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode())) {
			if (execution.getAllFailureExceptions().isEmpty()) {
				throw new ItemStreamException("Could not run job");
			}
			throw new ItemStreamException("Could not run job", execution.getAllFailureExceptions().get(0));
		}
		return execution;
	}

	private boolean isCompleted(JobExecution execution) {
		return execution.getStatus().isUnsuccessful() || execution.getStatus().equals(BatchStatus.COMPLETED);
	}

	private boolean isRunning(JobExecution execution) {
		return execution.isRunning() && keyReader.isOpen() && writer.isOpen();
	}

	@Override
	public synchronized void close() {
		if (isOpen()) {
			log.debug(String.format("Closing %s", name));
			if (jobExecution.isRunning()) {
				Await await = new Await();
				boolean executed;
				try {
					executed = await.await(() -> !isRunning(jobExecution), closeTimeout);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new ItemStreamException("Interruped while waiting for job to start", e);
				}
				if (!executed) {
					log.debug("Terminating step executions");
					jobExecution.getStepExecutions().forEach(StepExecution::setTerminateOnly);
				}
			}
			jobExecution = null;
			log.debug(String.format("Closed %s", name));
		}
	}

	@Override
	public synchronized T read() throws InterruptedException {
		T item;
		do {
			item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && writer.isOpen());
		return item;
	}

	/**
	 * 
	 * @param count number of items to read at once
	 * @return up to <code>count</code> items from the queue
	 */
	public synchronized List<T> read(int count) {
		List<T> items = new ArrayList<>(count);
		writer.getQueue().drainTo(items);
		return items;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return writer.getQueue().poll(timeout, unit);
	}

	private void initializeJobInfrastructure() throws Exception {
		if (transactionManager == null) {
			transactionManager = new ResourcelessTransactionManager();
		}
		if (jobRepository == null) {
			JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
			bean.setDataSource(dataSource());
			bean.setTransactionManager(transactionManager);
			bean.afterPropertiesSet();
			if (jobRepository == null) {
				try {
					jobRepository = bean.getObject();
				} catch (Exception e) {
					throw new ItemStreamException("Could not initialize job repository");
				}
				if (jobRepository == null) {
					throw new ItemStreamException("Job repository is null");
				}
			}
		}
		jobLauncher = new TaskExecutorJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
	}

	private DataSource dataSource() throws Exception {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setURL("jdbc:hsqldb:mem:" + name);
		BatchProperties.Jdbc jdbc = new BatchProperties.Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		BatchDataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(dataSource,
				jdbc);
		initializer.afterPropertiesSet();
		initializer.initializeDatabase();
		return dataSource;
	}

	private SimpleStepBuilder<K, K> step() {
		FaultTolerantStepBuilder<K, K> step = simpleStep().faultTolerant();
		keyReader = keyReader();
		step.reader(keyReader);
		step.processor(keyProcessor);
		writer = writer();
		step.writer(writer);
		if (threads > 1) {
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setMaxPoolSize(threads);
			taskExecutor.setCorePoolSize(threads);
			taskExecutor.setQueueCapacity(threads);
			taskExecutor.afterPropertiesSet();
			step.taskExecutor(taskExecutor);
		}
		step.skipLimit(skipLimit);
		step.retryLimit(retryLimit);
		skippableExceptions.forEach(step::skip);
		nonSkippableExceptions.forEach(step::noSkip);
		retriableExceptions.forEach(step::retry);
		nonRetriableExceptions.forEach(step::noRetry);
		return step;
	}

	private SimpleStepBuilder<K, K> simpleStep() {
		SimpleStepBuilder<K, K> step = new StepBuilder(name, jobRepository).chunk(chunkSize, transactionManager);
		if (isLive()) {
			FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(step);
			flushingStep.interval(flushInterval);
			flushingStep.idleTimeout(idleTimeout);
			return flushingStep;
		}
		return step;
	}

	private ProcessingItemWriter<K, T> writer() {
		BlockingQueue<T> queue = new LinkedBlockingQueue<>(queueCapacity);
		Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
		ProcessingItemWriter<K, T> processingWriter = new ProcessingItemWriter<>(valueReader(), queue);
		return processingWriter;
	}

	protected abstract ItemProcessor<Iterable<K>, Iterable<T>> valueReader();

	private KeyItemReader<K> keyReader() {
		if (isLive()) {
			return keyspaceNotificationReader();
		}
		return scanKeyReader();
	}

	private KeyspaceNotificationItemReader<K> keyspaceNotificationReader() {
		KeyspaceNotificationItemReader<K> reader = new KeyspaceNotificationItemReader<>(client, codec);
		reader.setDatabase(database);
		reader.setKeyPattern(keyPattern);
		reader.setKeyType(keyType);
		reader.setOrderingStrategy(orderingStrategy);
		reader.setQueueCapacity(notificationQueueCapacity);
		reader.setPollTimeout(pollTimeout);
		return reader;
	}

	public KeyScanItemReader<K> scanKeyReader() {
		KeyScanItemReader<K> reader = new KeyScanItemReader<>(client, codec);
		reader.setReadFrom(readFrom);
		reader.setLimit(scanCount);
		reader.setMatch(keyPattern);
		reader.setType(keyType == null ? null : keyType.getString());
		return reader;
	}

	public boolean isLive() {
		return mode == ReaderMode.LIVE;
	}

	public synchronized boolean isOpen() {
		return jobExecution != null;
	}

	public static DumpItemReader dump(AbstractRedisClient client) {
		return new DumpItemReader(client);
	}

	public static StructItemReader<String, String> struct(AbstractRedisClient client) {
		return struct(client, CodecUtils.STRING_CODEC);
	}

	public static <K, V> StructItemReader<K, V> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new StructItemReader<>(client, codec);
	}

	@SuppressWarnings("unchecked")
	public static List<Class<? extends Throwable>> defaultRetriableExceptions() {
		return modifiableList(RedisCommandTimeoutException.class);
	}

	@SuppressWarnings("unchecked")
	public static List<Class<? extends Throwable>> defaultNonRetriableExceptions() {
		return modifiableList(RedisCommandExecutionException.class);
	}

	@SuppressWarnings("unchecked")
	private static <T> List<T> modifiableList(T... elements) {
		return new ArrayList<>(Arrays.asList(elements));
	}

	public static KeyTypeItemReader<String, String> type(AbstractRedisClient client) {
		return type(client, CodecUtils.STRING_CODEC);
	}

	public static <K, V> KeyTypeItemReader<K, V> type(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new KeyTypeItemReader<>(client, codec);
	}

}
