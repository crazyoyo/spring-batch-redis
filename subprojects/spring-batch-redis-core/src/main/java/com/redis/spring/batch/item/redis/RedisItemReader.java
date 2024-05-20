package com.redis.spring.batch.item.redis;

import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.item.AbstractAsyncItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.Operation;
import com.redis.spring.batch.item.redis.common.OperationExecutor;
import com.redis.spring.batch.item.redis.reader.KeyNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.KeyScanNotificationItemReader;
import com.redis.spring.batch.item.redis.reader.MemKeyValue;
import com.redis.spring.batch.item.redis.reader.MemKeyValueRead;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class RedisItemReader<K, V, T> extends AbstractAsyncItemReader<K, T> {

	public enum ReaderMode {
		SCAN, LIVE, LIVE_SCAN
	}

	public static final int DEFAULT_POOL_SIZE = OperationExecutor.DEFAULT_POOL_SIZE;
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = KeyNotificationItemReader.DEFAULT_QUEUE_CAPACITY;
	public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;
	public static final ReaderMode DEFAULT_MODE = ReaderMode.SCAN;

	private final RedisCodec<K, V> codec;
	private final Operation<K, V, K, T> operation;

	private ReaderMode mode = DEFAULT_MODE;
	private int poolSize = DEFAULT_POOL_SIZE;
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private ReadFrom readFrom;
	private String keyPattern;
	private String keyType;
	private long scanCount;
	private int database;

	private AbstractRedisClient client;

	public RedisItemReader(RedisCodec<K, V> codec, Operation<K, V, K, T> operation) {
		setRetryLimit(DEFAULT_RETRY_LIMIT);
		this.codec = codec;
		this.operation = operation;
	}

	public Operation<K, V, K, T> getOperation() {
		return operation;
	}

	@Override
	protected FaultTolerantStepBuilder<K, K> faultTolerant(SimpleStepBuilder<K, K> step) {
		FaultTolerantStepBuilder<K, K> ftStep = super.faultTolerant(step);
		ftStep.skip(RedisCommandExecutionException.class);
		ftStep.noRetry(RedisCommandExecutionException.class);
		ftStep.noSkip(RedisCommandTimeoutException.class);
		ftStep.retry(RedisCommandTimeoutException.class);
		return ftStep;
	}

	@Override
	protected boolean isFlushing() {
		switch (mode) {
		case LIVE:
		case LIVE_SCAN:
			return true;
		default:
			return false;
		}
	}

	@Override
	protected ItemReader<K> reader() {
		switch (mode) {
		case LIVE:
			return notificationReader();
		case LIVE_SCAN:
			return scanAndNotificationReader();
		default:
			return scanReader();
		}
	}

	private ItemReader<K> scanAndNotificationReader() {
		return new KeyScanNotificationItemReader<>(client, codec, scanReader());
	}

	private IteratorItemReader<K> scanReader() {
		return new IteratorItemReader<>(ScanIterator.scan(connection().sync(), scanArgs()));
	}

	private KeyNotificationItemReader<K, V> notificationReader() {
		KeyNotificationItemReader<K, V> reader = new KeyNotificationItemReader<>(client, codec);
		reader.setName(getName() + "-key-notification-reader");
		reader.setQueueCapacity(notificationQueueCapacity);
		reader.setDatabase(database);
		reader.setKeyPattern(keyPattern);
		reader.setKeyType(keyType);
		reader.setPollTimeout(pollTimeout);
		return reader;
	}

	@Override
	protected OperationExecutor<K, V, K, T> writeProcessor() {
		return operationExecutor();
	}

	public OperationExecutor<K, V, K, T> operationExecutor() {
		Assert.notNull(client, getName() + ": Redis client not set");
		OperationExecutor<K, V, K, T> executor = new OperationExecutor<>(codec, operation);
		executor.setClient(client);
		executor.setPoolSize(poolSize);
		executor.setReadFrom(readFrom);
		return executor;
	}

	private StatefulRedisModulesConnection<K, V> connection() {
		return BatchUtils.connection(client, codec, readFrom);
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

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
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

	public ReaderMode getMode() {
		return mode;
	}

	public void setMode(ReaderMode mode) {
		this.mode = mode;
	}

}
