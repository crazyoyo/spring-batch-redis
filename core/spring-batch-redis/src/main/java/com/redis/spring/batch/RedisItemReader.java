package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;
import com.redis.spring.batch.writer.ProcessingItemWriter;
import com.redis.spring.batch.writer.QueueItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.Exceptions;
import io.micrometer.core.instrument.Metrics;

public abstract class RedisItemReader<K, V, T> implements PollableItemReader<T> {

	public enum Mode {
		SCAN, LIVE
	}

	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final String QUEUE_METER = "redis.batch.reader.queue.size";
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = KeyspaceNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final String MATCH_ALL = "*";
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	public static final Duration DEFAULT_FLUSH_INTERVAL = KeyspaceNotificationItemReader.DEFAULT_FLUSH_INTERVAL;
	// No idle timeout by default
	public static final Duration DEFAULT_IDLE_TIMEOUT = KeyspaceNotificationItemReader.DEFAULT_IDLE_TIMEOUT;
	public static final Mode DEFAULT_MODE = Mode.SCAN;
	public static final String DEFAULT_KEY_PATTERN = MATCH_ALL;

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private Mode mode = DEFAULT_MODE;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private int retryLimit = DEFAULT_RETRY_LIMIT;
	private List<Class<? extends Throwable>> skippableExceptions = defaultNonRetriableExceptions();
	private List<Class<? extends Throwable>> nonSkippableExceptions = defaultRetriableExceptions();
	private List<Class<? extends Throwable>> retriableExceptions = defaultRetriableExceptions();
	private List<Class<? extends Throwable>> nonRetriableExceptions = defaultNonRetriableExceptions();
	private int database;
	private int keyspaceNotificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private long scanCount;
	protected ItemProcessor<K, K> keyProcessor;
	private ReadFrom readFrom;
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private String keyPattern = DEFAULT_KEY_PATTERN;
	private String keyType;
	private Duration pollTimeout = KeyspaceNotificationItemReader.DEFAULT_POLL_TIMEOUT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private String name;
	private JobExecution jobExecution;
	private BlockingQueue<T> valueQueue;

	protected RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(String.format("%s-%s", ClassUtils.getShortName(getClass()), UUID.randomUUID().toString()));
		this.client = client;
		this.codec = codec;
	}

	private String pubSubPattern() {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (jobExecution == null) {
			PlatformTransactionManager txManager = new ResourcelessTransactionManager();
			JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
			bean.setDataSource(dataSource());
			bean.setTransactionManager(txManager);
			JobRepository jobRepository;
			try {
				bean.afterPropertiesSet();
				jobRepository = bean.getObject();
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize JobRepository", e);
			}
			if (jobRepository == null) {
				throw new ItemStreamException("Could not initialize JobRepository");
			}
			TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
			jobLauncher.setJobRepository(jobRepository);
			jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
			SimpleStepBuilder<K, K> step = step(jobRepository, txManager);
			Job job = new JobBuilder(name, jobRepository).start(step.build()).build();
			try {
				jobExecution = jobLauncher.run(job, new JobParameters());
			} catch (JobExecutionException e) {
				throw new ItemStreamException("Job execution failed", e);
			}
			boolean success;
			try {
				success = Await.await().until(jobExecution::isRunning);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ItemStreamException("Job interrupted", e);
			}
			if (!success) {
				List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
				if (!CollectionUtils.isEmpty(exceptions)) {
					throw new ItemStreamException("Job failed", Exceptions.unwrap(exceptions.get(0)));
				}
			}
		}
	}

	private DataSource dataSource() {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setURL("jdbc:hsqldb:mem:" + name);
		BatchProperties.Jdbc jdbc = new BatchProperties.Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		BatchDataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(dataSource,
				jdbc);
		try {
			initializer.afterPropertiesSet();
		} catch (Exception e) {
			throw new ItemStreamException("Could not initialize DataSource", e);
		}
		initializer.initializeDatabase();
		return dataSource;
	}

	private KeyScanArgs scanArgs() {
		KeyScanArgs args = new KeyScanArgs();
		if (scanCount > 0) {
			args.limit(scanCount);
		}
		if (keyPattern != null) {
			args.match(keyPattern);
		}
		if (keyType != null) {
			args.type(keyType);
		}
		return args;
	}

	private SimpleStepBuilder<K, K> step(JobRepository jobRepository, PlatformTransactionManager txManager) {
		FaultTolerantStepBuilder<K, K> step = baseStep(jobRepository, txManager).faultTolerant();
		step.reader(keyReader());
		step.processor(keyProcessor);
		step.writer(writer());
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

	private ItemReader<K> keyReader() {
		if (isLive()) {
			return keyspaceNotificationReader();
		}
		StatefulRedisModulesConnection<K, V> connection = ConnectionUtils.connection(client, codec, readFrom);
		ScanIterator<K> iterator = ScanIterator.scan(ConnectionUtils.sync(connection), scanArgs());
		return new IteratorItemReader<>(iterator);
	}

	public KeyspaceNotificationItemReader<K> keyspaceNotificationReader() {
		KeyspaceNotificationItemReader<K> reader = new KeyspaceNotificationItemReader<>(client, codec, pubSubPattern());
		reader.setKeyType(keyType);
		reader.setPollTimeout(pollTimeout);
		reader.setQueueCapacity(keyspaceNotificationQueueCapacity);
		return reader;
	}

	private ProcessingItemWriter<K, T> writer() {
		valueQueue = new LinkedBlockingQueue<>(queueCapacity);
		Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), valueQueue);
		return new ProcessingItemWriter<>(new FunctionItemProcessor<>(this::values), new QueueItemWriter<>(valueQueue));
	}

	private SimpleStepBuilder<K, K> baseStep(JobRepository jobRepository, PlatformTransactionManager txManager) {
		SimpleStepBuilder<K, K> step = new StepBuilder(name, jobRepository).chunk(chunkSize, txManager);
		if (isLive()) {
			FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(step);
			flushingStep.interval(flushInterval);
			flushingStep.idleTimeout(idleTimeout);
			return flushingStep;
		}
		return step;
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		jobExecution = null;
	}

	public abstract Chunk<T> values(Chunk<? extends K> chunk);

	@Override
	public T read() throws InterruptedException {
		T item;
		do {
			item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && jobExecution.isRunning());
		return item;
	}

	/**
	 * 
	 * @param count number of items to read at once
	 * @return up to <code>count</code> items from the queue
	 */
	public List<T> read(int count) {
		List<T> items = new ArrayList<>(count);
		valueQueue.drainTo(items);
		return items;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return valueQueue.poll(timeout, unit);
	}

	public boolean isLive() {
		return mode == Mode.LIVE;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public RedisCodec<K, V> getCodec() {
		return codec;
	}

	public Mode getMode() {
		return mode;
	}

	public String getName() {
		return name;
	}

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(Duration interval) {
		this.flushInterval = interval;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration timeout) {
		this.idleTimeout = timeout;
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

	public void setMode(Mode mode) {
		this.mode = mode;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public void setKeyPattern(String glob) {
		this.keyPattern = glob;
	}

	public void setKeyType(DataType type) {
		setKeyType(type == null ? null : type.getString());
	}

	public void setKeyType(String type) {
		this.keyType = type;
	}

	public int getDatabase() {
		return database;
	}

	public Duration getPollTimeout() {
		return pollTimeout;
	}

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

	public int getKeyspaceNotificationQueueCapacity() {
		return keyspaceNotificationQueueCapacity;
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

	public String getKeyType() {
		return keyType;
	}

	public void setKeyspaceNotificationQueueCapacity(int capacity) {
		this.keyspaceNotificationQueueCapacity = capacity;
	}

	public void setDatabase(int database) {
		this.database = database;
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
