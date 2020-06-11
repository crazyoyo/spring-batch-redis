package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.springframework.core.convert.converter.Converter;

public class RedisKeyspaceNotificationProducer<K, V> extends AbstractKeyspaceNotificationProducer<K> implements RedisPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> connection;
    private final K pattern;

    public RedisKeyspaceNotificationProducer(StatefulRedisPubSubConnection<K, V> connection, K pattern, Converter<K, K> keyExtractor) {
        super(keyExtractor);
        this.connection = connection;
        this.pattern = pattern;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void open() {
        connection.addListener(this);
        connection.sync().psubscribe(pattern);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void close() {
        connection.sync().punsubscribe(pattern);
        connection.removeListener(this);
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

}
