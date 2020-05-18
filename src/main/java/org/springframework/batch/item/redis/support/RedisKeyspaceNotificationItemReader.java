package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.function.BiFunction;

public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> implements RedisPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> connection;

    public RedisKeyspaceNotificationItemReader(StatefulRedisPubSubConnection<K, V> connection, K[] patterns, int queueCapacity, long queuePollingTimeout, BiFunction<K, V, V> keyExtractor) {
        super(patterns, queueCapacity, queuePollingTimeout, keyExtractor);
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

    public static RedisKeyspaceNotificationItemReaderBuilder builder() {
        return new RedisKeyspaceNotificationItemReaderBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisKeyspaceNotificationItemReaderBuilder extends KeyspaceNotificationItemReaderBuilder {

        private RedisClient client;
        private String[] patterns;
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

        public RedisKeyspaceNotificationItemReader<String, String> build() {
            return new RedisKeyspaceNotificationItemReader<>(client.connectPubSub(), patterns, queueCapacity, queuePollingTimeout, DEFAULT_KEY_EXTRACTOR);
        }
    }

}
