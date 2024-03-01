package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class KeyspaceNotificationProcessingTask<K, V> extends AbstractChunkProcessingTask<K, V> {

	public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofMillis(50);
	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);

	private final Function<String, K> keyEncoder;
	private final BlockingQueue<String> keyQueue;
	private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

	public KeyspaceNotificationProcessingTask(RedisCodec<K, ?> codec, BlockingQueue<String> keyQueue,
			Function<Iterable<K>, Iterable<V>> valueReader, BlockingQueue<V> valueQueue) {
		super(valueReader, valueQueue);
		this.keyEncoder = CodecUtils.stringKeyFunction(codec);
		this.keyQueue = keyQueue;
	}

	public void setFlushInterval(Duration interval) {
		this.flushInterval = interval;
	}

	public void setIdleTimeout(Duration timeout) {
		this.idleTimeout = timeout;
	}

	@Override
	public void execute() throws InterruptedException {
		try (ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor()) {
			ScheduledFuture<?> future = executor.scheduleAtFixedRate(this::safeFlush, flushInterval.toMillis(),
					flushInterval.toMillis(), TimeUnit.MILLISECONDS);
			String key;
			while ((key = keyQueue.poll(idleTimeout.toMillis(), TimeUnit.MILLISECONDS)) != null) {
				add(keyEncoder.apply(key));
			}
			future.cancel(true);
		}
	}

	private void safeFlush() {
		try {
			flush();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}

}