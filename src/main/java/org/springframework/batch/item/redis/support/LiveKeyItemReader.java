package org.springframework.batch.item.redis.support;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.hybhub.util.concurrent.ConcurrentSetBlockingQueue;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiveKeyItemReader<K, V> extends KeyItemReader<K, V>
	implements RedisPubSubListener<K, V>, RedisClusterPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> pubSubConnection;

    private final long queuePollingTimeout;

    private final BlockingQueue<K> queue;

    private final K pubSubPattern;

    private final Converter<K, K> keyExtractor;

    private boolean stopped;

    @Getter
    private boolean running;

    public LiveKeyItemReader(StatefulConnection<K, V> connection,
	    Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commands, long scanCount, String scanMatch,
	    StatefulRedisPubSubConnection<K, V> pubSubConnection, int queueCapacity, long queuePollingTimeout,
	    K pubSubPattern, Converter<K, K> keyExtractor) {
	super(connection, commands, scanCount, scanMatch);
	Assert.notNull(pubSubConnection, "A PubSub connection is required.");
	Assert.notNull(pubSubPattern, "A PubSub channel pattern is required.");
	Assert.notNull(keyExtractor, "A key extractor is required.");
	this.pubSubConnection = pubSubConnection;
	this.queue = new ConcurrentSetBlockingQueue<>(queueCapacity);
	this.queuePollingTimeout = queuePollingTimeout;
	this.pubSubPattern = pubSubPattern;
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
    protected synchronized void doOpen() {
	super.doOpen();
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
	super.doClose();
    }

    public void stop() {
	this.stopped = true;
    }

    @Override
    protected synchronized K doRead() throws Exception {
	if (queue.isEmpty()) {
	    K key = super.doRead();
	    if (key != null) {
		return key;
	    }
	}
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

}
