package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.FlushingChunkProvider;
import com.redis.spring.batch.common.FlushingStepBuilder;
import com.redis.spring.batch.common.JobFactory;
import com.redis.spring.batch.reader.AbstractPollableItemReader;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.AwaitTimeoutException;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.Exceptions;
import io.micrometer.core.instrument.Metrics;

public abstract class RedisItemReader<K, V, T> extends AbstractPollableItemReader<T> {

	public enum Mode {
		SCAN, LIVE
	}

	public static final String QUEUE_METER = "redis.batch.reader.queue.size";
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = KeyspaceNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final String MATCH_ALL = "*";
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	public static final Mode DEFAULT_MODE = Mode.SCAN;
	public static final String DEFAULT_KEY_PATTERN = MATCH_ALL;
	public static final int DEFAULT_SKIP_LIMIT = 0;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

	private static final Duration DEFAULT_FLUSHING_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;
	private static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

	private final Log log = LogFactory.getLog(RedisItemReader.class);
	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private Mode mode = DEFAULT_MODE;
	private int skipLimit = DEFAULT_SKIP_LIMIT;
	private int retryLimit = DEFAULT_RETRY_LIMIT;
	private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private int database;
	private int keyspaceNotificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private long scanCount;
	protected ItemProcessor<K, K> keyProcessor;
	private ReadFrom readFrom;
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private String keyPattern = DEFAULT_KEY_PATTERN;
	private String keyType;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private JobFactory jobFactory;
	private JobExecution jobExecution;
	private ItemReader<K> keyReader;
	private BlockingQueue<T> queue;

	protected RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
	}

	private String pubSubPattern() {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (jobExecution == null) {
			if (jobFactory == null) {
				jobFactory = new JobFactory();
			}
			jobFactory.afterPropertiesSet();
			keyReader = keyReader();
			SimpleStepBuilder<K, K> step = step(jobFactory.step(getName(), chunkSize));
			if (threads > 1) {
				step.taskExecutor(JobFactory.threadPoolTaskExecutor(threads));
			}
			step.reader(keyReader);
			step.processor(keyProcessor);
			step.writer(writer());
			FaultTolerantStepBuilder<K, K> ftStep = step.faultTolerant();
			ftStep.retryLimit(retryLimit);
			ftStep.skipLimit(skipLimit);
			ftStep.skip(RedisCommandExecutionException.class);
			ftStep.noRetry(RedisCommandExecutionException.class);
			ftStep.noSkip(RedisCommandTimeoutException.class);
			ftStep.retry(RedisCommandTimeoutException.class);
			Job job = jobFactory.job(getName()).start(ftStep.build()).build();
			jobExecution = jobFactory.runAsync(job);
			try {
				Await.await().until(() -> jobExecution.isRunning() || jobExecution.getStatus().isUnsuccessful());
			} catch (AwaitTimeoutException e) {
				List<Throwable> exceptions = jobExecution.getAllFailureExceptions();
				if (!CollectionUtils.isEmpty(exceptions)) {
					throw new JobExecutionException("Job failed", Exceptions.unwrap(exceptions.get(0)));
				}
			}
		}
	}

	private SimpleStepBuilder<K, K> step(SimpleStepBuilder<K, K> step) {
		if (isLive()) {
			FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(step);
			flushingStep.flushingInterval(flushingInterval);
			flushingStep.idleTimeout(idleTimeout);
			return flushingStep;
		}
		return step;
	}

	public JobExecution getJobExecution() {
		return jobExecution;
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

	public ItemReader<K> keyReader() {
		if (isLive()) {
			String pattern = pubSubPattern();
			KeyspaceNotificationItemReader<K> reader = new KeyspaceNotificationItemReader<>(client, codec, pattern);
			reader.setName(getName() + "-keyspace-notification-reader");
			reader.setKeyType(keyType);
			reader.setPollTimeout(pollTimeout);
			reader.setQueueCapacity(keyspaceNotificationQueueCapacity);
			return reader;
		}
		StatefulRedisModulesConnection<K, V> connection = ConnectionUtils.connection(client, codec, readFrom);
		ScanIterator<K> scanIterator = ScanIterator.scan(ConnectionUtils.sync(connection), scanArgs());
		IteratorItemReader<K> reader = new IteratorItemReader<>(scanIterator);
		if (threads > 1) {
			return new SynchronizedItemReader<>(reader);
		}
		return reader;
	}

	private ItemWriter<K> writer() {
		queue = new LinkedBlockingQueue<>(queueCapacity);
		Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
		return new Writer();
	}

	private class Writer implements ItemWriter<K> {

		@Override
		public void write(Chunk<? extends K> chunk) throws InterruptedException {
			Chunk<T> values = values(chunk);
			if (values != null) {
				for (T value : values) {
					queue.put(value);
				}
			}
		}

	}

	@Override
	protected synchronized void doClose() throws Exception {
		if (jobExecution != null) {
			Await.await().untilFalse(jobExecution::isRunning);
			if (!queue.isEmpty()) {
				log.warn(String.format("%s queue still contains %,d elements", getName(), queue.size()));
			}
			jobExecution = null;
		}
	}

	@Override
	protected boolean isEnd() {
		return jobExecution == null || !jobExecution.isRunning();
	}

	protected abstract Chunk<T> values(Chunk<? extends K> chunk);

	/**
	 * 
	 * @param count number of items to read at once
	 * @return up to <code>count</code> items from the queue
	 */
	public List<T> read(int count) {
		List<T> items = new ArrayList<>(count);
		queue.drainTo(items, count);
		return items;
	}

	@Override
	protected T doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public BlockingQueue<T> getQueue() {
		return queue;
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

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public int getRetryLimit() {
		return retryLimit;
	}

	public void setRetryLimit(int retryLimit) {
		this.retryLimit = retryLimit;
	}

	public int getSkipLimit() {
		return skipLimit;
	}

	public void setSkipLimit(int skipLimit) {
		this.skipLimit = skipLimit;
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

	public void setReadFrom(ReadFrom readFrom) {
		this.readFrom = readFrom;
	}

	public void setKeyPattern(String glob) {
		this.keyPattern = glob;
	}

	public void setKeyType(String type) {
		this.keyType = type;
	}

	public int getDatabase() {
		return database;
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

	public ItemReader<K> getKeyReader() {
		return keyReader;
	}

	public void setJobFactory(JobFactory jobInfrastructure) {
		this.jobFactory = jobInfrastructure;
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

	public static KeyTypeItemReader<String, String> type(AbstractRedisClient client) {
		return type(client, CodecUtils.STRING_CODEC);
	}

	public static <K, V> KeyTypeItemReader<K, V> type(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new KeyTypeItemReader<>(client, codec);
	}

}
