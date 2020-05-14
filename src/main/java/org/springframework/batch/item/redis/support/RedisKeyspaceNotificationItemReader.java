package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Builder;
import org.springframework.util.Assert;

import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;

public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> implements RedisPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> connection;

    @Builder
    public RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<K, V> connection, BlockingQueue<V> queue, long pollingTimeout, K[] patterns, BiFunction<K,V,V> keyExtractor) {
        super(queue, pollingTimeout, patterns, keyExtractor);
        Assert.notNull(connection, "A connection is required.");
        this.connection = connection;
    }

    @Override
    protected void open(K[] patterns) {
        connection.addListener(this);
        connection.sync().psubscribe(patterns);
    }

    @Override
    protected void close(K[] patterns) {
        connection.sync().punsubscribe(patterns);
        connection.removeListener(this);
    }

    @Override
    public void message(K channel, V message) {
        enqueue(channel, message);
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
}
