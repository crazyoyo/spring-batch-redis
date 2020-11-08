package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemReader;

import io.lettuce.core.codec.RedisCodec;

@SuppressWarnings("unchecked")
public class KeyValueItemReaderBuilder<K, V, B extends KeyValueItemReaderBuilder<K, V, B, T>, T>
	extends RedisConnectionBuilder<K, V, B> {

    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final int DEFAULT_BATCH_SIZE = 50;
    public static final int DEFAULT_QUEUE_CAPACITY = 1000;
    public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;

    protected ItemReader<K> keyReader;
    protected int threadCount = DEFAULT_THREAD_COUNT;
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    protected long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

    public KeyValueItemReaderBuilder(RedisCodec<K, V> codec) {
	super(codec);
    }

    public B keyReader(ItemReader<K> keyReader) {
	this.keyReader = keyReader;
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

    public B queuePollingTimeout(long queuePollingTimeout) {
	this.queuePollingTimeout = queuePollingTimeout;
	return (B) this;
    }

}
