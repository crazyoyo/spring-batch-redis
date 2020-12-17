package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Builder;

public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> {

    private final StatefulRedisPubSubConnection<K, V> connection;
    private final KeyspaceNotificationListener listener = new KeyspaceNotificationListener();

    @Builder
    public RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<K, V> connection, KeyspaceNotificationReaderOptions<K> options) {
        super(options);
        this.connection = connection;
    }

    @Override
    protected void subscribe(K pattern) {
        connection.addListener(listener);
        connection.sync().psubscribe(pattern);
    }

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
