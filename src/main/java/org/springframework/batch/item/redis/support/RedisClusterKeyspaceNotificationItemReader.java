package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.function.BiFunction;

public class RedisClusterKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> implements RedisClusterPubSubListener<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> connection;

    public RedisClusterKeyspaceNotificationItemReader(StatefulRedisClusterPubSubConnection<K, V> connection, K[] patterns, int queueCapacity, long queuePollingTimeout, BiFunction<K, V, V> keyExtractor) {
        super(patterns, queueCapacity, queuePollingTimeout, keyExtractor);
        this.connection = connection;
    }

    @Override
    protected void open(K[] patterns) {
        connection.addListener(this);
        connection.setNodeMessagePropagation(true);
        connection.sync().masters().commands().psubscribe(patterns);
    }

    @Override
    protected void close(K[] patterns) {
        connection.sync().masters().commands().punsubscribe(patterns);
        connection.removeListener(this);
    }

    @Override
    public void message(RedisClusterNode node, K channel, V message) {
        enqueue(channel, message);
    }

    @Override
    public void message(RedisClusterNode node, K pattern, K channel, V message) {
        enqueue(channel, message);
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

    public static RedisClusterKeyspaceNotificationItemReaderBuilder builder() {
        return new RedisClusterKeyspaceNotificationItemReaderBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisClusterKeyspaceNotificationItemReaderBuilder extends KeyspaceNotificationItemReaderBuilder {

        private RedisClusterClient client;
        private String[] patterns;
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

        public RedisClusterKeyspaceNotificationItemReader<String,String> build() {
            return new RedisClusterKeyspaceNotificationItemReader<>(client.connectPubSub(), patterns, queueCapacity, queuePollingTimeout, DEFAULT_KEY_EXTRACTOR);
        }
    }
}
