package com.redis.spring.batch.item.redis;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.item.BlockingQueueItemWriter;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyEvent;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Operation;
import com.redis.spring.batch.item.redis.common.OperationExecutor;
import com.redis.spring.batch.item.redis.reader.KeyEventStatus;
import com.redis.spring.batch.item.redis.reader.KeyNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.KeyScanItemReader;
import com.redis.spring.batch.item.redis.reader.KeyScanNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.KeyValueRead;
import com.redis.spring.batch.item.redis.reader.KeyValueStructRead;
import com.redis.spring.batch.item.redis.reader.RedisScanSizeEstimator;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V, T> extends AbstractAsyncItemReader<KeyEvent<K>, KeyValue<K, T>> {

	public enum ReaderMode {
		SCAN, LIVE, LIVEONLY
	}

	public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = KeyNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final ReaderMode DEFAULT_MODE = ReaderMode.SCAN;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;
	public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

	private final RedisCodec<K, V> codec;
	private final BiPredicate<K, K> keyEquals;
	private final Operation<K, V, KeyEvent<K>, KeyValue<K, T>> operation;

	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private ReaderMode mode = DEFAULT_MODE;
	private int poolSize = DEFAULT_POOL_SIZE;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private ReadFrom readFrom;
	private String keyPattern;
	private String keyType;
	private long scanCount;
	private int database;

	private AbstractRedisClient client;
	private BlockingQueue<KeyValue<K, T>> queue;

	public RedisItemReader(RedisCodec<K, V> codec, Operation<K, V, KeyEvent<K>, KeyValue<K, T>> operation) {
		setName(ClassUtils.getShortName(getClass()));
		this.codec = codec;
		this.keyEquals = BatchUtils.keyEqualityPredicate(codec);
		this.operation = operation;
	}

	@Override
	protected SimpleStepBuilder<KeyEvent<K>, KeyEvent<K>> stepBuilder() {
		SimpleStepBuilder<KeyEvent<K>, KeyEvent<K>> step = super.stepBuilder();
		if (mode == ReaderMode.SCAN) {
			return step;
		}
		FlushingStepBuilder<KeyEvent<K>, KeyEvent<K>> flushingStep = new FlushingStepBuilder<>(step);
		flushingStep.flushInterval(flushInterval);
		flushingStep.idleTimeout(idleTimeout);
		return flushingStep;
	}

	@Override
	protected ItemReader<KeyEvent<K>> reader() {
		switch (mode) {
		case LIVEONLY:
			return notificationReader();
		case LIVE:
			return scanNotificationReader();
		default:
			return scanReader();
		}
	}

	@Override
	protected boolean jobRunning() {
		return super.jobRunning() && readerOpen();
	}

	@SuppressWarnings("unchecked")
	private boolean readerOpen() {
		switch (mode) {
		case LIVE:
		case LIVEONLY:
			return getReader() != null && ((KeyNotificationItemReader<K, V>) getReader()).isOpen();
		default:
			return true;
		}
	}

	private KeyScanNotificationItemReader<K, V> scanNotificationReader() {
		KeyScanNotificationItemReader<K, V> reader = new KeyScanNotificationItemReader<>(client, codec, scanReader());
		configure(reader);
		return reader;
	}

	private KeyScanItemReader<K, V> scanReader() {
		KeyScanItemReader<K, V> reader = new KeyScanItemReader<>(client, codec);
		reader.setReadFrom(readFrom);
		reader.setScanArgs(scanArgs());
		return reader;
	}

	private KeyNotificationItemReader<K, V> notificationReader() {
		KeyNotificationItemReader<K, V> reader = new KeyNotificationItemReader<>(client, codec);
		configure(reader);
		return reader;
	}

	private void configure(KeyNotificationItemReader<K, V> reader) {
		reader.setName(getName() + "-key-reader");
		reader.setQueueCapacity(notificationQueueCapacity);
		reader.setDatabase(database);
		reader.setKeyPattern(keyPattern);
		reader.setKeyType(keyType);
		reader.setPollTimeout(pollTimeout);
		reader.addListener(this::keyNotification);
	}

	private void keyNotification(KeyEvent<K> notification, KeyEventStatus status) {
		if (status == KeyEventStatus.ACCEPTED) {
			queue.removeIf(t -> keyEquals.test(t.getKey(), notification.getKey()));
		}
	}

	@Override
	protected KeyValue<K, T> doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	protected ItemWriter<KeyEvent<K>> writer() {
		queue = new LinkedBlockingQueue<>(queueCapacity);
		return new BlockingQueueItemWriter<>(operationExecutor(), queue);
	}

	public OperationExecutor<K, V, KeyEvent<K>, KeyValue<K, T>> operationExecutor() {
		Assert.notNull(client, getName() + ": Redis client not set");
		OperationExecutor<K, V, KeyEvent<K>, KeyValue<K, T>> executor = new OperationExecutor<>(codec, operation);
		executor.setClient(client);
		executor.setPoolSize(poolSize);
		executor.setReadFrom(readFrom);
		return executor;
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

	public RedisScanSizeEstimator scanSizeEstimator() {
		return RedisScanSizeEstimator.from(this);
	}

	public static RedisItemReader<byte[], byte[], byte[]> dump() {
		return new RedisItemReader<>(ByteArrayCodec.INSTANCE, KeyValueRead.dump(ByteArrayCodec.INSTANCE));
	}

	public static RedisItemReader<String, String, Object> type() {
		return type(StringCodec.UTF8);
	}

	public static <K, V> RedisItemReader<K, V, Object> type(RedisCodec<K, V> codec) {
		return new RedisItemReader<>(codec, KeyValueRead.type(codec));
	}

	public static RedisItemReader<String, String, Object> struct() {
		return struct(StringCodec.UTF8);
	}

	public static <K, V> RedisItemReader<K, V, Object> struct(RedisCodec<K, V> codec) {
		return new RedisItemReader<>(codec, new KeyValueStructRead<>(codec));
	}

	public Operation<K, V, KeyEvent<K>, KeyValue<K, T>> getOperation() {
		return operation;
	}

	public BlockingQueue<KeyValue<K, T>> getQueue() {
		return queue;
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

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int size) {
		this.poolSize = size;
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public void setKeyPattern(String pattern) {
		this.keyPattern = pattern;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String type) {
		this.keyType = type;
	}

	public long getScanCount() {
		return scanCount;
	}

	public void setScanCount(long count) {
		this.scanCount = count;
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

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public ReaderMode getMode() {
		return mode;
	}

	public void setMode(ReaderMode mode) {
		this.mode = mode;
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

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int capacity) {
		this.queueCapacity = capacity;
	}

}
