package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;

public class RedisKeyspaceNotificationItemReader<V> extends AbstractKeyspaceNotificationItemReader<V> implements RedisPubSubListener<String, V> {

    @Getter
    @Setter
    private StatefulRedisPubSubConnection<String, V> connection;

    private RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<String, V> connection, Integer database, BlockingQueue<V> queue, Duration pollingTimeout) {
        super(database, queue, pollingTimeout);
        Assert.notNull(connection, "A connection is required.");
        this.connection = connection;
    }

    @Builder
    private RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<String, V> connection, Integer database, Integer queueCapacity, Duration pollingTimeout) {
        this(connection, database, createQueue(queueCapacity), pollingTimeout);
    }

    @Override
    protected void open(String channel) {
        connection.addListener(this);
        connection.sync().psubscribe(channel);
    }

    @Override
    protected void close(String channel) {
        connection.sync().punsubscribe(channel);
        connection.removeListener(this);
    }

    @Override
    public void message(String channel, V message) {
        message(message);
    }

    @Override
    public void message(String pattern, String channel, V message) {
        message(message);
    }

    @Override
    public void subscribed(String channel, long count) {
    }

    @Override
    public void psubscribed(String pattern, long count) {
    }

    @Override
    public void unsubscribed(String channel, long count) {
    }

    @Override
    public void punsubscribed(String pattern, long count) {
    }
}
