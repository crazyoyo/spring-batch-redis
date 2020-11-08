package org.springframework.batch.item.redis.support;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.springframework.batch.item.redis.support.KeyItemReader.KeyItemReaderBuilder;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.hybhub.util.concurrent.ConcurrentSetBlockingQueue;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiveKeyItemReader<K, V> extends AbstractProgressReportingItemReader<K>
	implements RedisPubSubListener<K, V>, RedisClusterPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> pubSubConnection;
    private final long queuePollingTimeout;
    private final BlockingQueue<K> queue;
    private final K pubSubPattern;
    private final Converter<K, K> keyExtractor;
    private boolean stopped;
    @Getter
    private boolean running;

    public LiveKeyItemReader(StatefulRedisPubSubConnection<K, V> connection, K pattern, Converter<K, K> keyExtractor,
	    int queueCapacity, long queuePollingTimeout) {
	setName(ClassUtils.getShortName(getClass()));
	Assert.notNull(connection, "A PubSub connection is required.");
	Assert.notNull(pattern, "A PubSub channel pattern is required.");
	Assert.notNull(keyExtractor, "A key extractor is required.");
	this.pubSubConnection = connection;
	this.queue = new ConcurrentSetBlockingQueue<>(queueCapacity);
	this.queuePollingTimeout = queuePollingTimeout;
	this.pubSubPattern = pattern;
	this.keyExtractor = keyExtractor;
    }

    @Override
    public void message(K channel, V message) {
	notification(channel);
    }

    @Override
    public void message(K pattern, K channel, V message) {
	message(channel, message);
    }

    @Override
    public void subscribed(K channel, long count) {
    }

    @Override
    public void psubscribed(K pattern, long count) {
    }

    @Override
    public void unsubscribed(K channel, long count) {
    }

    @Override
    public void punsubscribed(K pattern, long count) {
    }

    @Override
    public void message(RedisClusterNode node, K channel, V message) {
	notification(channel);
    }

    @Override
    public void message(RedisClusterNode node, K pattern, K channel, V message) {
	notification(channel);
    }

    @Override
    public void subscribed(RedisClusterNode node, K channel, long count) {
    }

    @Override
    public void psubscribed(RedisClusterNode node, K pattern, long count) {
    }

    @Override
    public void unsubscribed(RedisClusterNode node, K channel, long count) {
    }

    @Override
    public void punsubscribed(RedisClusterNode node, K pattern, long count) {
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doOpen() throws InterruptedException, ExecutionException, TimeoutException {
	if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
	    StatefulRedisClusterPubSubConnection<K, V> clusterPubSubConnection = (StatefulRedisClusterPubSubConnection<K, V>) pubSubConnection;
	    clusterPubSubConnection.addListener((RedisClusterPubSubListener<K, V>) this);
	    clusterPubSubConnection.setNodeMessagePropagation(true);
	    clusterPubSubConnection.sync().upstream().commands().psubscribe(pubSubPattern);
	} else {
	    pubSubConnection.addListener(this);
	    pubSubConnection.sync().psubscribe(pubSubPattern);
	}
	this.running = true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doClose() {
	if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
	    StatefulRedisClusterPubSubConnection<K, V> clusterPubSubConnection = (StatefulRedisClusterPubSubConnection<K, V>) pubSubConnection;
	    clusterPubSubConnection.sync().upstream().commands().punsubscribe(pubSubPattern);
	    clusterPubSubConnection.removeListener((RedisClusterPubSubListener<K, V>) this);
	} else {
	    pubSubConnection.sync().punsubscribe(pubSubPattern);
	    pubSubConnection.removeListener(this);
	}
    }

    public void stop() {
	this.stopped = true;
    }

    @Override
    protected synchronized K doRead() throws Exception {
	K key;
	do {
	    key = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
	} while (key == null && !stopped);
	return key;
    }

    private void notification(K notification) {
	K key = keyExtractor.convert(notification);
	if (key == null) {
	    return;
	}
	try {
	    queue.put(key);
	} catch (InterruptedException e) {
	    log.debug("Interrupted while trying to enqueue key", e);
	}
    }

    @Override
    public Long getTotal() {
	return null;
    }

    public static LiveKeyItemReaderBuilder<String, String> builder() {
	return new LiveKeyItemReaderBuilder<>(StringCodec.UTF8,
		b -> "__keyspace@" + b.uri().getDatabase() + "__:" + b.scanMatch, new StringChannelConverter());
    }

    @Setter
    @Accessors(fluent = true)
    public static class LiveKeyItemReaderBuilder<K, V>
	    extends RedisConnectionBuilder<K, V, LiveKeyItemReaderBuilder<K, V>> {

	public static final int DEFAULT_QUEUE_CAPACITY = 1000;
	public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;

	private final Converter<K, K> keyExtractor;
	private final Function<LiveKeyItemReaderBuilder<K, V>, K> pubSubPatternProvider;
	private String scanMatch = KeyItemReaderBuilder.DEFAULT_SCAN_MATCH;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

	public LiveKeyItemReaderBuilder(RedisCodec<K, V> codec,
		Function<LiveKeyItemReaderBuilder<K, V>, K> pubSubPatternProvider, Converter<K, K> keyExtractor) {
	    super(codec);
	    this.pubSubPatternProvider = pubSubPatternProvider;
	    this.keyExtractor = keyExtractor;
	}

	public LiveKeyItemReader<K, V> build() {
	    return new LiveKeyItemReader<>(pubSubConnection(), pubSubPatternProvider.apply(this), keyExtractor,
		    queueCapacity, queuePollingTimeout);
	}

    }

}
