package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.item.support.SynchronizedItemReader;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.FlushingChunkProvider;
import com.redis.spring.batch.common.FlushingStepBuilder;
import com.redis.spring.batch.common.JobFactory;
import com.redis.spring.batch.reader.AbstractPollableItemReader;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyNotificationItemReader;
import com.redis.spring.batch.reader.MemKeyValue;
import com.redis.spring.batch.reader.MemKeyValueRead;
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V, T> extends AbstractPollableItemReader<T> {

	public enum ReaderMode {
		SNAPSHOT, LIVE
	}

	public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;
	public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = KeyNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final ReaderMode DEFAULT_MODE = ReaderMode.SNAPSHOT;

	private final Log log = LogFactory.getLog(RedisItemReader.class);

	private final RedisCodec<K, V> codec;
	protected final Operation<K, V, K, T> operation;
	private AbstractRedisClient client;
	private JobFactory jobFactory;
	private ItemProcessor<K, K> keyProcessor;
	private int poolSize = DEFAULT_POOL_SIZE;
	private ReaderMode mode = DEFAULT_MODE;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private int retryLimit = DEFAULT_RETRY_LIMIT;
	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private ReadFrom readFrom;
	private String keyPattern;
	private String keyType;
	private long scanCount;
	private int database;

	private JobExecution jobExecution;
	private BlockingQueue<T> queue;
	private OperationExecutor<K, V, K, T> operationExecutor;
	private ItemReader<K> reader;

	public RedisItemReader(RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
		this.codec = codec;
		this.operation = operation;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		Assert.notNull(client, getName() + ": Redis client not set");
		if (jobFactory == null) {
			jobFactory = new JobFactory();
		}
		jobFactory.afterPropertiesSet();
		if (operationExecutor == null) {
			operationExecutor = new OperationExecutor<>(codec, operation);
			operationExecutor.setClient(client);
			operationExecutor.setPoolSize(poolSize);
			operationExecutor.setReadFrom(readFrom);
			operationExecutor.afterPropertiesSet();
		}
		if (queue == null) {
			queue = new LinkedBlockingQueue<>();
		}
		if (jobExecution == null) {
			SimpleStepBuilder<K, K> step = step();
			Job job = jobFactory.jobBuilder(getName()).start(step.build()).build();
			jobExecution = jobFactory.runAsync(job);
			try {
				Await.await().until(() -> jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			} catch (TimeoutException e) {
				List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
				if (!CollectionUtils.isEmpty(exceptions)) {
					throw new JobExecutionException("Job execution unsuccessful", exceptions.get(0));
				}
			}
		}
	}

	private FaultTolerantStepBuilder<K, K> step() {
		SimpleStepBuilder<K, K> step = stepBuilder();
		reader = reader();
		step.reader(reader);
		step.processor(keyProcessor);
		step.writer(new Writer());
		if (threads > 1) {
			step.taskExecutor(JobFactory.threadPoolTaskExecutor(threads));
			if (!isLive()) {
				step.reader(new SynchronizedItemReader<>(reader));
			}
		}
		FaultTolerantStepBuilder<K, K> ftStep = step.faultTolerant();
		ftStep.retryLimit(retryLimit);
		ftStep.skipLimit(skipLimit);
		ftStep.skip(RedisCommandExecutionException.class);
		ftStep.noRetry(RedisCommandExecutionException.class);
		ftStep.noSkip(RedisCommandTimeoutException.class);
		ftStep.retry(RedisCommandTimeoutException.class);
		return ftStep;
	}

	private SimpleStepBuilder<K, K> stepBuilder() {
		SimpleStepBuilder<K, K> step = jobFactory.step(getName(), chunkSize);
		if (isLive()) {
			FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(step);
			flushingStep.flushInterval(flushInterval);
			flushingStep.idleTimeout(idleTimeout);
			return flushingStep;
		}
		return step;
	}

	private boolean isLive() {
		return mode == ReaderMode.LIVE;
	}

	private ItemReader<K> reader() {
		if (isLive()) {
			KeyNotificationItemReader<K, V> notificationReader = new KeyNotificationItemReader<>(client, codec);
			notificationReader.setName(getName() + "-key-notification-reader");
			notificationReader.setQueueCapacity(notificationQueueCapacity);
			notificationReader.setDatabase(database);
			notificationReader.setKeyPattern(keyPattern);
			notificationReader.setKeyType(keyType);
			notificationReader.setPollTimeout(pollTimeout);
			return notificationReader;
		}
		ScanIterator<K> scanIterator = ScanIterator.scan(connection().sync(), scanArgs());
		return new IteratorItemReader<>(scanIterator);
	}

	public List<T> read(Iterable<? extends K> keys) {
		return operationExecutor.apply(keys);
	}

	private class Writer implements ItemWriter<K> {

		@Override
		public void write(Chunk<? extends K> chunk) throws Exception {
			List<T> elements = read(chunk);
			for (T element : elements) {
				queue.put(element);
			}
		}

	}

	private StatefulRedisModulesConnection<K, V> connection() {
		return BatchUtils.connection(client, codec, readFrom);
	}

	@Override
	protected synchronized void doClose() throws TimeoutException, InterruptedException {
		if (!queue.isEmpty()) {
			log.warn(String.format("%s queue still contains %,d elements", getName(), queue.size()));
		}
		if (jobExecution != null) {
			Await.await().untilFalse(jobExecution::isRunning);
			jobExecution = null;
		}
		if (operationExecutor != null) {
			operationExecutor.close();
			operationExecutor = null;
		}
	}

	@Override
	public boolean isRunning() {
		return jobExecution != null && jobExecution.isRunning();
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

	/**
	 * 
	 * @param count number of items to read at once
	 * @return up to <code>count</code> items from the queue
	 */
	public List<T> read(int count) {
		List<T> items = new ArrayList<>(count);
		if (queue != null) {
			queue.drainTo(items, count);
		}
		return items;
	}

	@Override
	protected T doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public static KeyComparisonItemReader<String, String> compare() {
		return compare(StringCodec.UTF8);
	}

	public static <K, V> KeyComparisonItemReader<K, V> compare(RedisCodec<K, V> codec) {
		return new KeyComparisonItemReader<>(codec, MemKeyValueRead.struct(codec), MemKeyValueRead.struct(codec));
	}

	public static KeyComparisonItemReader<String, String> compareQuick() {
		return compareQuick(StringCodec.UTF8);
	}

	public static <K, V> KeyComparisonItemReader<K, V> compareQuick(RedisCodec<K, V> codec) {
		return new KeyComparisonItemReader<>(codec, MemKeyValueRead.type(codec), MemKeyValueRead.type(codec));
	}

	public static RedisItemReader<byte[], byte[], MemKeyValue<byte[], byte[]>> dump() {
		return new RedisItemReader<>(ByteArrayCodec.INSTANCE, MemKeyValueRead.dump());
	}

	public static RedisItemReader<String, String, MemKeyValue<String, Object>> type() {
		return type(StringCodec.UTF8);
	}

	public static <K, V> RedisItemReader<K, V, MemKeyValue<K, Object>> type(RedisCodec<K, V> codec) {
		return new RedisItemReader<>(codec, MemKeyValueRead.type(codec));
	}

	public static RedisItemReader<String, String, MemKeyValue<String, Object>> struct() {
		return struct(StringCodec.UTF8);
	}

	public static <K, V> RedisItemReader<K, V, MemKeyValue<K, Object>> struct(RedisCodec<K, V> codec) {
		return new RedisItemReader<>(codec, MemKeyValueRead.struct(codec));
	}

	public ItemReader<K> getReader() {
		return reader;
	}

	public Operation<K, V, K, T> getOperation() {
		return operation;
	}

	public RedisCodec<K, V> getCodec() {
		return codec;
	}

	public AbstractRedisClient getClient() {
		return client;
	}

	public void setClient(AbstractRedisClient client) {
		this.client = client;
	}

	public JobFactory getJobFactory() {
		return jobFactory;
	}

	public void setJobFactory(JobFactory jobFactory) {
		this.jobFactory = jobFactory;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

	public ReaderMode getMode() {
		return mode;
	}

	public void setMode(ReaderMode mode) {
		this.mode = mode;
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public void setKeyPattern(String keyPattern) {
		this.keyPattern = keyPattern;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long scanCount) {
		this.scanCount = scanCount;
	}

	public ReadFrom getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public void setNotificationQueueCapacity(int notificationQueueCapacity) {
		this.notificationQueueCapacity = notificationQueueCapacity;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public JobExecution getJobExecution() {
		return jobExecution;
	}

	public ItemProcessor<K, K> getKeyProcessor() {
		return keyProcessor;
	}

	public void setKeyProcessor(ItemProcessor<K, K> keyProcessor) {
		this.keyProcessor = keyProcessor;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
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

	public void setIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

}
