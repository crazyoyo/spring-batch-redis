package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.SetBlockingQueue;
import com.redis.spring.batch.reader.AbstractKeyspaceNotificationListener;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyScanProcessingTask;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationListener;
import com.redis.spring.batch.reader.KeyspaceNotificationProcessingTask;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ProcessingTask;
import com.redis.spring.batch.reader.RedisClusterKeyspaceNotificationListener;
import com.redis.spring.batch.reader.RedisKeyspaceNotificationListener;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;
import com.redis.spring.batch.util.IdentityOperator;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.Futures;
import io.micrometer.core.instrument.Metrics;

public abstract class RedisItemReader<K, V, T> implements PollableItemReader<T> {

	public enum Mode {
		SCAN, LIVE
	}

	public static final String QUEUE_METER = "redis.batch.reader.queue.size";
	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final int DEFAULT_THREADS = 1;
	public static final int DEFAULT_CHUNK_SIZE = 50;
	public static final String MATCH_ALL = "*";
	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
	public static final Duration DEFAULT_FLUSH_INTERVAL = KeyspaceNotificationProcessingTask.DEFAULT_FLUSH_INTERVAL;
	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
	// No idle timeout by default
	public static final Duration DEFAULT_IDLE_TIMEOUT = KeyspaceNotificationProcessingTask.DEFAULT_IDLE_TIMEOUT;
	public static final Mode DEFAULT_MODE = Mode.SCAN;
	public static final String DEFAULT_KEY_PATTERN = MATCH_ALL;

	private static final Predicate<? super Future<Long>> futureRunning = Predicate.not(Future::isDone);

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;

	private Mode mode = DEFAULT_MODE;
	private int database;
	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
	private long scanCount;
	protected UnaryOperator<K> keyOperator = new IdentityOperator<>();
	private ReadFrom readFrom;
	private int threads = DEFAULT_THREADS;
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;
	private String keyPattern = DEFAULT_KEY_PATTERN;
	private String keyType;
	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private BlockingQueue<T> valueQueue;
	private List<Future<Long>> futures;
	private KeyspaceNotificationListener keyspaceNotificationListener;
	private ExecutorService executor;

	protected RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.client = client;
		this.codec = codec;
	}

	private String pubSubPattern() {
		return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (futures == null) {
			valueQueue = new LinkedBlockingQueue<>(queueCapacity);
			Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), valueQueue);
			executor = Executors.newFixedThreadPool(threads);
			futures = IntStream.range(0, threads).mapToObj(taskSupplier()).map(executor::submit)
					.collect(Collectors.toList());
		}
	}

	private IntFunction<ProcessingTask> taskSupplier() {
		if (isLive()) {
			BlockingQueue<String> keyQueue = new SetBlockingQueue<>(
					new LinkedBlockingQueue<>(notificationQueueCapacity), notificationQueueCapacity);
			keyspaceNotificationListener = keyspaceNotificationListener(keyQueue);
			keyspaceNotificationListener.start();
			return i -> keyspaceNotificationListenerTask(keyQueue);
		}
		StatefulRedisModulesConnection<K, V> connection = ConnectionUtils.connection(client, codec, readFrom);
		ScanIterator<K> iterator = ScanIterator.scan(ConnectionUtils.sync(connection), scanArgs());
		return i -> new KeyScanProcessingTask<>(iterator, this::values, valueQueue);
	}

	private KeyspaceNotificationProcessingTask<K, T> keyspaceNotificationListenerTask(BlockingQueue<String> keyQueue) {
		KeyspaceNotificationProcessingTask<K, T> task = new KeyspaceNotificationProcessingTask<>(codec, keyQueue,
				this::values, valueQueue);
		task.setChunkSize(chunkSize);
		task.setFlushInterval(flushInterval);
		task.setIdleTimeout(idleTimeout);
		task.setKeyOperator(keyOperator);
		return task;
	}

	public KeyspaceNotificationListener keyspaceNotificationListener(BlockingQueue<String> queue) {
		String pattern = pubSubPattern();
		AbstractKeyspaceNotificationListener listener;
		if (client instanceof RedisClusterClient) {
			listener = new RedisClusterKeyspaceNotificationListener((RedisClusterClient) client, pattern, queue);
		} else {
			listener = new RedisKeyspaceNotificationListener((RedisClient) client, queue, pattern);
		}
		listener.setKeyType(keyType);
		return listener;
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

	@Override
	public synchronized void close() throws ItemStreamException {
		if (futures != null) {
			if (keyspaceNotificationListener != null) {
				keyspaceNotificationListener.stop();
				keyspaceNotificationListener = null;
			}
			Futures.awaitAll(3, TimeUnit.SECONDS, futures.toArray(new Future[0]));
			futures = null;
			if (executor != null) {
				executor.close();
				executor = null;
			}
		}
	}

	protected abstract Chunk<T> values(Chunk<K> chunk);

	@Override
	public T read() throws InterruptedException {
		T item;
		do {
			item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && futures.stream().anyMatch(futureRunning));
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

	public UnaryOperator<K> getKeyOperator() {
		return keyOperator;
	}

	public void setKeyOperator(UnaryOperator<K> processor) {
		this.keyOperator = processor;
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

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
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

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
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
