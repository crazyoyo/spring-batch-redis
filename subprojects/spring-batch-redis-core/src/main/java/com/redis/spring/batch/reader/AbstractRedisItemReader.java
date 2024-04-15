package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
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
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.AwaitTimeoutException;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.Exceptions;

public abstract class AbstractRedisItemReader<K, V, T> extends AbstractPollableItemReader<T> {

	public enum ReaderMode {
		SNAPSHOT, LIVE
	}

	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final Duration DEFAULT_FLUSHING_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;
	public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = KeyNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final ReaderMode DEFAULT_MODE = ReaderMode.SNAPSHOT;

	private final Log log = LogFactory.getLog(AbstractRedisItemReader.class);

	protected final RedisCodec<K, V> codec;
	protected AbstractRedisClient client;
	private JobFactory jobFactory;
	private ReaderMode mode = ReaderMode.SNAPSHOT;
	private ItemProcessor<K, K> keyProcessor;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int threads = DEFAULT_THREADS;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private int retryLimit = DEFAULT_RETRY_LIMIT;
	private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private int database;
	private ReadFrom readFrom;
	private String keyPattern;
	private String keyType;
	private long scanCount;

	private JobExecution jobExecution;
	private BlockingQueue<T> queue;

	protected AbstractRedisItemReader(RedisCodec<K, V> codec) {
		this.codec = codec;
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

	@Override
	protected synchronized void doOpen() {
		Assert.notNull(client, "Redis client not set");
		if (jobFactory == null) {
			jobFactory = new JobFactory();
		}
		if (queue == null) {
			queue = new LinkedBlockingQueue<>();
		}
		if (jobExecution == null) {
			SimpleStepBuilder<K, K> step = step();
			Job job = jobFactory.jobBuilder(getName()).start(step.build()).build();
			try {
				jobExecution = jobFactory.runAsync(job);
			} catch (Exception e) {
				throw new ItemStreamException("Could not run job", e);
			}
			try {
				Await.await().until(() -> jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ItemStreamException("Interrupted while waiting for job to start", e);
			} catch (AwaitTimeoutException e) {
				List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
				if (!CollectionUtils.isEmpty(exceptions)) {
					throw new ItemStreamException("Job failed", Exceptions.unwrap(exceptions.get(0)));
				}
			}
		}
	}

	private FaultTolerantStepBuilder<K, K> step() {
		SimpleStepBuilder<K, K> step = stepBuilder();
		ItemReader<? extends K> reader = reader();
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
			flushingStep.flushingInterval(flushingInterval);
			flushingStep.idleTimeout(idleTimeout);
			return flushingStep;
		}
		return step;
	}

	private boolean isLive() {
		return mode == ReaderMode.LIVE;
	}

	private ItemReader<? extends K> reader() {
		if (isLive()) {
			KeyNotificationItemReader<K, V> reader = new KeyNotificationItemReader<>(client, codec);
			reader.setName(getName() + "-key-notification-reader");
			reader.setQueueCapacity(notificationQueueCapacity);
			reader.setDatabase(database);
			reader.setKeyPattern(keyPattern);
			reader.setKeyType(keyType);
			reader.setPollTimeout(pollTimeout);
			return reader;
		}
		ScanIterator<K> scanIterator = ScanIterator.scan(connection().sync(), scanArgs());
		return new IteratorItemReader<>(scanIterator);
	}

	private class Writer implements ItemWriter<K> {

		@Override
		public void write(Chunk<? extends K> chunk) throws Exception {
			List<T> elements = execute(chunk);
			for (T element : elements) {
				queue.put(element);
			}
		}

	}

	protected abstract List<T> execute(Iterable<? extends K> keys);

	protected StatefulRedisModulesConnection<K, V> connection() {
		return BatchUtils.connection(client, codec, readFrom);
	}

	@Override
	protected synchronized void doClose() throws InterruptedException {
		if (!queue.isEmpty()) {
			log.warn(String.format("%s queue still contains %,d elements", getName(), queue.size()));
		}
		if (jobExecution != null) {
			Await.await().untilFalse(jobExecution::isRunning);
			jobExecution = null;
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

	public Duration getFlushingInterval() {
		return flushingInterval;
	}

	public void setFlushingInterval(Duration flushingInterval) {
		this.flushingInterval = flushingInterval;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

}
