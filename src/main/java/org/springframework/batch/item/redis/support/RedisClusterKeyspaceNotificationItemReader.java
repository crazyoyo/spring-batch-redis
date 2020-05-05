package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;

public class RedisClusterKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> implements RedisClusterPubSubListener<K, V> {

    @Getter
    @Setter
    private StatefulRedisClusterPubSubConnection<K, V> connection;

    public RedisClusterKeyspaceNotificationItemReader(StatefulRedisClusterPubSubConnection<K, V> connection, K channel, BlockingQueue<V> queue, Duration pollingTimeout) {
        super(channel, queue, pollingTimeout);
        this.connection = connection;
    }

    @Builder
    public RedisClusterKeyspaceNotificationItemReader(StatefulRedisClusterPubSubConnection<K, V> connection, K channel, Integer queueCapacity, Duration pollingTimeout) {
        super(channel, queueCapacity, pollingTimeout);
        Assert.notNull(connection, "A connection is required.");
        this.connection = connection;
    }

    @Override
    protected void open(K channel) {
        connection.addListener(this);
        connection.setNodeMessagePropagation(true);
        connection.sync().masters().commands().psubscribe(channel);
    }

    @Override
    protected void close(K channel) {
        connection.sync().masters().commands().punsubscribe(channel);
        connection.removeListener(this);
    }

    @Override
    public void message(RedisClusterNode node, K channel, V message) {
        message(message);
    }

    @Override
    public void message(RedisClusterNode node, K pattern, K channel, V message) {
        message(message);
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
}
