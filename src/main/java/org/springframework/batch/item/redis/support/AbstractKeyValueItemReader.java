package org.springframework.batch.item.redis.support;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKeyValueItemReader<T extends KeyValue<?>> extends
		AbstractItemCountingItemStreamItemReader<T> implements BoundedItemReader<T>, FlushableItemStreamReader<T> {

	@Getter
	private final ItemReader<String> keyReader;
	private final BlockingQueue<T> queue;
	private final long queuePollingTimeout;
	private final GenericObjectPool<? extends StatefulConnection<String, String>> pool;
	private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commandsProvider;
	private final long commandTimeout;
	private final Transfer<String, String> transfer;
	private TransferExecution<String, String> transferExecution;

	protected AbstractKeyValueItemReader(ItemReader<String> keyReader, int threads, int batch, int queueCapacity,
			long queuePollingTimeout, GenericObjectPool<? extends StatefulConnection<String, String>> pool,
			Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> commands,
			long commandTimeout) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(keyReader, "A key reader is required.");
		Assert.isTrue(threads > 0, "Thread count must be greater than zero.");
		Assert.isTrue(batch > 0, "Batch size must be greater than zero.");
		Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero.");
		Assert.isTrue(queuePollingTimeout > 0, "Queue polling timeout must be greater than zero.");
		Assert.notNull(pool, "A connection pool is required.");
		Assert.notNull(commands, "A command provider is required.");
		Assert.isTrue(commandTimeout > 0, "Command timeout must be greater than zero.");
		this.keyReader = keyReader;
		this.transfer = Transfer.<String, String>builder().name("value-reader").reader(keyReader).writer(this::write)
				.threads(threads).batch(batch).build();
		this.queue = new LinkedBlockingDeque<>(queueCapacity);
		this.queuePollingTimeout = queuePollingTimeout;
		this.pool = pool;
		this.commandsProvider = commands;
		this.commandTimeout = commandTimeout;
	}

	@Override
	protected void doOpen() {
		this.transferExecution = transfer.execute();
	}

	private void write(List<? extends String> keys) throws Exception {
		for (T value : read(keys)) {
			queue.removeIf(v -> v.getKey().equals(value.getKey()));
			queue.put(value);
		}
	}

	@Override
	protected T doRead() throws Exception {
		T item;
		do {
			item = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
		} while (item == null && transferExecution.isRunning());
		return item;
	}

	@Override
	public void flush() {
		transferExecution.flush();
	}

	public List<T> read(List<? extends String> keys) throws Exception {
		try (StatefulConnection<String, String> connection = pool.borrowObject()) {
			BaseRedisAsyncCommands<String, String> commands = this.commandsProvider.apply(connection);
			commands.setAutoFlushCommands(false);
			try {
				return read(keys, commands);
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	protected abstract List<T> read(List<? extends String> keys, BaseRedisAsyncCommands<String, String> commands)
			throws Exception;

	protected <F> F get(RedisFuture<F> future) throws InterruptedException, ExecutionException, TimeoutException {
		if (future == null) {
			return null;
		}
		return future.get(commandTimeout, TimeUnit.SECONDS);
	}

	protected long getTtl(RedisFuture<Long> future) throws InterruptedException, ExecutionException, TimeoutException {
		Long ttl = get(future);
		if (ttl == null) {
			return 0;
		}
		return ttl;
	}

	@Override
	protected void doClose() throws ItemStreamException {
		if (!queue.isEmpty()) {
			log.warn("Closing {} - {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
		}
		if (transferExecution.isRunning()) {
			boolean cancelled = transferExecution.getFuture().cancel(false);
			if (!cancelled) {
				log.warn("Could not gracefully shut down transfer");
			}
		}
		transferExecution = null;
	}

	@Override
	public int available() {
		if (keyReader instanceof BoundedItemReader) {
			return ((BoundedItemReader<String>) keyReader).available();
		}
		return 0;
	}

	@SuppressWarnings("unchecked")
	public static class KeyValueItemReaderBuilder<B extends KeyValueItemReaderBuilder<B>>
			extends RedisConnectionBuilder<B> {

		public static final int DEFAULT_THREAD_COUNT = 1;
		public static final int DEFAULT_BATCH_SIZE = 50;
		public static final int DEFAULT_QUEUE_CAPACITY = 1000;
		public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;
		public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;
		public static final long DEFAULT_SCAN_COUNT = 1000;
		public static final String DEFAULT_SCAN_MATCH = "*";
		public static final int DEFAULT_KEY_SAMPLE_SIZE = 100;

		protected int threadCount = DEFAULT_THREAD_COUNT;
		protected int batchSize = DEFAULT_BATCH_SIZE;
		protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
		protected int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;
		protected long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;
		private boolean live;
		private long scanCount = DEFAULT_SCAN_COUNT;
		private String scanMatch = DEFAULT_SCAN_MATCH;
		private int keySampleSize = DEFAULT_KEY_SAMPLE_SIZE;

		public B live(boolean live) {
			this.live = live;
			return (B) this;
		}

		public B threads(int threads) {
			this.threadCount = threads;
			return (B) this;
		}

		public B batch(int batch) {
			this.batchSize = batch;
			return (B) this;
		}

		public B queueCapacity(int queueCapacity) {
			this.queueCapacity = queueCapacity;
			return (B) this;
		}

		public B notificationQueueCapacity(int notificationQueueCapacity) {
			this.notificationQueueCapacity = notificationQueueCapacity;
			return (B) this;
		}

		public B queuePollingTimeout(long queuePollingTimeout) {
			this.queuePollingTimeout = queuePollingTimeout;
			return (B) this;
		}

		public B scanCount(long scanCount) {
			this.scanCount = scanCount;
			return (B) this;
		}

		public B keySampleSize(int keySampleSize) {
			this.keySampleSize = keySampleSize;
			return (B) this;
		}

		public B scanMatch(String scanMatch) {
			this.scanMatch = scanMatch;
			return (B) this;
		}

		protected ItemReader<String> keyReader() {
			if (live) {
				String pattern = "__keyspace@" + uri().getDatabase() + "__:" + scanMatch;
				return new LiveKeyItemReader(pubSubConnection(), pattern, notificationQueueCapacity,
						queuePollingTimeout);
			}
			return new KeyItemReader(connection(), sync(), async(), timeout(), scanCount, scanMatch, keySampleSize);
		}

	}

}
