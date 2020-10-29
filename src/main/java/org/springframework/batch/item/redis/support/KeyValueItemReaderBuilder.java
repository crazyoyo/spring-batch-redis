package org.springframework.batch.item.redis.support;

import java.util.function.Function;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.codec.RedisCodec;

@SuppressWarnings("unchecked")
public class KeyValueItemReaderBuilder<K, V, B extends KeyValueItemReaderBuilder<K, V, B, T>, T>
		extends RedisConnectionBuilder<K, V, B> {

	public KeyValueItemReaderBuilder(RedisCodec<K, V> codec) {
		super(codec);
	}

	public static final int DEFAULT_THREAD_COUNT = 1;
	public static final int DEFAULT_BATCH_SIZE = 50;
	public static final long DEFAULT_SCAN_COUNT = 1000;
	public static final String DEFAULT_SCAN_MATCH = "*";
	public static final int DEFAULT_CAPACITY = 10000;
	public static final long DEFAULT_POLLING_TIMEOUT = 100;

	protected int threadCount = DEFAULT_THREAD_COUNT;
	protected int batchSize = DEFAULT_BATCH_SIZE;
	protected int queueCapacity = DEFAULT_CAPACITY;
	protected long queuePollingTimeout = DEFAULT_POLLING_TIMEOUT;
	protected long scanCount = DEFAULT_SCAN_COUNT;
	protected String scanMatch = DEFAULT_SCAN_MATCH;
	private boolean live;

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

	public B queuePollingTimeout(long queuePollingTimeout) {
		this.queuePollingTimeout = queuePollingTimeout;
		return (B) this;
	}

	public B scanCount(long scanCount) {
		this.scanCount = scanCount;
		return (B) this;
	}

	public B scanMatch(String scanMatch) {
		this.scanMatch = scanMatch;
		return (B) this;
	}

	public B live(boolean live) {
		this.live = live;
		return (B) this;
	}

	protected KeyItemReader<K, V> keyReader(Function<B, K> pubSubPatternProvider, Converter<K, K> keyExtractor) {
		if (live) {
			return new LiveKeyItemReader<>(connection(), sync(), scanCount, scanMatch, pubSubConnection(),
					queueCapacity, queuePollingTimeout, pubSubPatternProvider.apply((B) this), keyExtractor);
		}
		return new KeyItemReader<>(connection(), sync(), scanCount, scanMatch);
	}

	public static <B extends KeyValueItemReaderBuilder<String, String, B, ?>> Function<B, String> stringPubSubPatternProvider() {
		return b -> "__keyspace@" + b.uri().getDatabase() + "__:" + b.scanMatch;
	}

}
