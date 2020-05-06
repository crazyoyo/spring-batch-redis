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

public class RedisClusterKeyspaceNotificationItemReader<V> extends AbstractKeyspaceNotificationItemReader<V> implements RedisClusterPubSubListener<String, V> {

    @Getter
    @Setter
    private StatefulRedisClusterPubSubConnection<String, V> connection;

    @Builder
    private RedisClusterKeyspaceNotificationItemReader(StatefulRedisClusterPubSubConnection<String, V> connection, Integer database, BlockingQueue<V> queue, Duration pollingTimeout) {
        super(database, queue, pollingTimeout);
        this.connection = connection;
    }

    @Builder
    private RedisClusterKeyspaceNotificationItemReader(StatefulRedisClusterPubSubConnection<String, V> connection, Integer database, Integer queueCapacity, Duration pollingTimeout) {
        super(database, queueCapacity, pollingTimeout);
        Assert.notNull(connection, "A connection is required.");
        this.connection = connection;
    }

    @Override
    protected void open(String channel) {
        connection.addListener(this);
        connection.setNodeMessagePropagation(true);
        connection.sync().masters().commands().psubscribe(channel);
    }

    @Override
    protected void close(String channel) {
        connection.sync().masters().commands().punsubscribe(channel);
        connection.removeListener(this);
    }

    @Override
    public void message(RedisClusterNode node, String channel, V message) {
        message(message);
    }

    @Override
    public void message(RedisClusterNode node, String pattern, String channel, V message) {
        message(message);
    }

    @Override
    public void subscribed(RedisClusterNode node, String channel, long count) {
    }

    @Override
    public void psubscribed(RedisClusterNode node, String pattern, long count) {
    }

    @Override
    public void unsubscribed(RedisClusterNode node, String channel, long count) {
    }

    @Override
    public void punsubscribed(RedisClusterNode node, String pattern, long count) {
    }
}
