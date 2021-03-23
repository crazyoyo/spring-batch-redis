package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> {

    private final StatefulRedisPubSubConnection<K, V> connection;
    private final KeyspaceNotificationListener listener = new KeyspaceNotificationListener();

    public RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<K, V> connection, K pubSubPattern, Converter<K, K> keyExtractor, int queueCapacity) {
        super(pubSubPattern, keyExtractor, queueCapacity);
        Assert.notNull(connection, "A pub/sub connection is required.");
        this.connection = connection;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void subscribe(K pattern) {
        connection.addListener(listener);
        connection.sync().psubscribe(pattern);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void unsubscribe(K pubSubPattern) {
        connection.sync().punsubscribe(pubSubPattern);
        connection.removeListener(listener);
    }

    private class KeyspaceNotificationListener extends RedisPubSubAdapter<K, V> {

        @Override
        public void message(K channel, V message) {
            notification(channel, message);
        }

        @Override
        public void message(K pattern, K channel, V message) {
            notification(channel, message);
        }
    }


}
