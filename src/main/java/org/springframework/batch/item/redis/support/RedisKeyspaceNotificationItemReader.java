package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;

public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> implements RedisPubSubListener<K, V> {

    @Getter
    @Setter
    private StatefulRedisPubSubConnection<K, V> connection;

    public RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<K, V> connection, K channel, BlockingQueue<V> queue, Duration pollingTimeout) {
        super(channel, queue, pollingTimeout);
        Assert.notNull(connection, "A connection is required.");
        this.connection = connection;
    }

    @Builder
    public RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<K, V> connection, K channel, Integer queueCapacity, Duration pollingTimeout) {
        this(connection, channel, createQueue(queueCapacity), pollingTimeout);
    }

    @Override
    protected void open(K channel) {
        connection.addListener(this);
        connection.sync().psubscribe(channel);
    }

    @Override
    protected void close(K channel) {
        connection.sync().punsubscribe(channel);
        connection.removeListener(this);
    }

    @Override
    public void message(K channel, V message) {
        message(message);
    }

    @Override
    public void message(K pattern, K channel, V message) {
        message(message);
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
}
