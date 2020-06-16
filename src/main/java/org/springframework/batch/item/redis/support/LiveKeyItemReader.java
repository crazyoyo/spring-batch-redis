package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class LiveKeyItemReader<K, V> extends KeyItemReader<K, V> implements RedisPubSubListener<K, V>, RedisClusterPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> pubSubConnection;
    private final long queuePollingTimeout;
    private final BlockingQueue<K> queue;
    private final K pubSubPattern;
    private final Converter<K, K> keyExtractor;

    private boolean stopped;
    @Getter
    private boolean running;

    public LiveKeyItemReader(StatefulConnection<K, V> connection, Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> commands, ScanArgs scanArgs, StatefulRedisPubSubConnection<K, V> pubSubConnection, int queueCapacity, long queuePollingTimeout, K pubSubPattern, Converter<K, K> keyExtractor) {
        super(connection, commands, scanArgs);
        this.pubSubConnection = pubSubConnection;
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
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
    protected synchronized void doOpen() {
        super.doOpen();
        if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
            StatefulRedisClusterPubSubConnection<K, V> clusterPubSubConnection = (StatefulRedisClusterPubSubConnection<K, V>) pubSubConnection;
            clusterPubSubConnection.addListener((RedisClusterPubSubListener<K, V>) this);
            clusterPubSubConnection.setNodeMessagePropagation(true);
            clusterPubSubConnection.sync().masters().commands().psubscribe(pubSubPattern);
        } else {
            pubSubConnection.addListener(this);
            pubSubConnection.sync().psubscribe(pubSubPattern);
        }
        this.running = true;
    }

    @Override
    protected synchronized void doClose() {
        if (pubSubConnection instanceof StatefulRedisClusterPubSubConnection) {
            StatefulRedisClusterPubSubConnection<K, V> clusterPubSubConnection = (StatefulRedisClusterPubSubConnection<K, V>) pubSubConnection;
            clusterPubSubConnection.sync().masters().commands().punsubscribe(pubSubPattern);
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
