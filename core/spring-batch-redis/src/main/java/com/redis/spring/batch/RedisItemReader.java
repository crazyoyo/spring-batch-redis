package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

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
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.Futures;
import io.micrometer.core.instrument.Metrics;

public abstract class RedisItemReader<K, V, T> implements ItemStreamReader<T>, PollableItemReader<T> {

	public enum ReaderMode {
		SCAN, LIVE
	}

	public static final String QUEUE_METER = "redis.batch.reader.queue.size";

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	public static final int DEFAULT_THREADS = 1;

	public static final int DEFAULT_CHUNK_SIZE = 50;

	public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

	public static final Duration DEFAULT_IDLE_TIMEOUT = KeyspaceNotificationItemReader.DEFAULT_IDLE_TIMEOUT;

	public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = KeyspaceNotificationItemReader.DEFAULT_QUEUE_CAPACITY;

	public static final OrderingStrategy DEFAULT_ORDERING = KeyspaceNotificationItemReader.DEFAULT_ORDERING;

	public static final ReaderMode DEFAULT_MODE = ReaderMode.SCAN;

	private final AbstractRedisClient client;

	private final RedisCodec<K, V> codec;

	private ReaderMode mode = DEFAULT_MODE;

	private int database;

	private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

	private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

	private long scanCount;

	protected ItemProcessor<K, K> keyProcessor;

	private ReadFrom readFrom;

	private int threads = DEFAULT_THREADS;

	private int chunkSize = DEFAULT_CHUNK_SIZE;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

	private String keyPattern;

	private DataType keyType;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

	private BlockingQueue<T> queue;

	private List<Task> tasks;

	private List<Future<Long>> futures;

	protected RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
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

	public void setScanCount(long count) {
		this.scanCount = count;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(Duration interval) {
		this.flushInterval = interval;
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

	public int getDatabase() {
		return database;
	}

	public Duration getPollTimeout() {
		return pollTimeout;
	}

	public void setPollTimeout(Duration timeout) {
		this.pollTimeout = timeout;
	}

	public OrderingStrategy getOrderingStrategy() {
		return orderingStrategy;
	}

	public int getNotificationQueueCapacity() {
		return notificationQueueCapacity;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(Duration timeout) {
		this.idleTimeout = timeout;
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

	public void setNotificationQueueCapacity(int capacity) {
		this.notificationQueueCapacity = capacity;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public void setOrderingStrategy(OrderingStrategy strategy) {
		this.orderingStrategy = strategy;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (!isOpen()) {
			KeyItemReader<K> keyReader = keyReader();
			keyReader.open(executionContext);
			queue = new LinkedBlockingQueue<>(queueCapacity);
			Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
			tasks = IntStream.range(0, threads).boxed().map(i -> new Task(keyReader)).collect(Collectors.toList());
			ExecutorService executor = Executors.newFixedThreadPool(threads);
			futures = tasks.stream().map(executor::submit).collect(Collectors.toList());
		}
	}

	@Override
	public void close() throws ItemStreamException {
		if (isOpen()) {
			boolean completed = Futures.awaitAll(3, TimeUnit.SECONDS, futures.toArray(new Future[0]));
			if (!completed) {
				tasks.forEach(Task::stop);
				Futures.awaitAll(3, TimeUnit.SECONDS, futures.toArray(new Future[0]));
			}
			futures = null;
		}
	}

	public abstract Iterable<T> process(Iterable<K> chunk) throws Exception;

	private class Task implements Callable<Long> {

		private final List<K> keys = new ArrayList<>();
		private final AtomicLong count = new AtomicLong();
		private final KeyItemReader<K> keyReader;
		private boolean stopped;
		private ScheduledFuture<?> scheduledFuture;

		public Task(KeyItemReader<K> keyReader) {
			this.keyReader = keyReader;
			if (isLive()) {
				ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(threads);
				scheduledFuture = scheduledExecutor.scheduleAtFixedRate(this::safeFlush, flushInterval.toMillis(),
						flushInterval.toMillis(), TimeUnit.MILLISECONDS);
			}
		}

		@Override
		public Long call() throws Exception {
			K key;
			while ((key = keyReader.read()) != null && !stopped) {
				keys.add(key);
				if (keys.size() >= chunkSize) {
					flush();
				}
			}
			if (!stopped) {
				flush();
			}
			if (scheduledFuture != null) {
				scheduledFuture.cancel(true);
			}
			return count.get();
		}

		private void safeFlush() {
			try {
				flush();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private void flush() throws Exception {
			synchronized (keys) {
				for (T item : process(keys)) {
					if (stopped) {
						return;
					}
					queue.put(item);
					count.incrementAndGet();
				}
				keys.clear();
			}
		}

		public void stop() {
			this.stopped = true;
		}

	}

	@Override
	public synchronized T read() throws InterruptedException {
		T item;
		do {
			item = queue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
		} while (item == null && !isDone());
		return item;
	}

	private boolean isDone() {
		for (Future<Long> future : futures) {
			if (!future.isDone()) {
				return false;
			}
		}
		return true;
	}

	public boolean isOpen() {
		return futures != null;
	}

	/**
	 * 
	 * @param count number of items to read at once
	 * @return up to <code>count</code> items from the queue
	 */
	public synchronized List<T> read(int count) {
		List<T> items = new ArrayList<>(count);
		queue.drainTo(items);
		return items;
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	private KeyItemReader<K> keyReader() {
		if (isLive()) {
			KeyspaceNotificationItemReader<K> reader = new KeyspaceNotificationItemReader<>(client, codec);
			reader.setDatabase(database);
			reader.setKeyPattern(keyPattern);
			reader.setKeyType(keyType);
			reader.setOrderingStrategy(orderingStrategy);
			reader.setIdleTimeout(idleTimeout);
			reader.setQueueCapacity(notificationQueueCapacity);
			return reader;
		}
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
