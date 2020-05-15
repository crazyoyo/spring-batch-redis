package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.Builder;
import lombok.NonNull;

import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;

public class RedisClusterKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> implements RedisClusterPubSubListener<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> connection;

    @Builder
    public RedisClusterKeyspaceNotificationItemReader(@NonNull StatefulRedisClusterPubSubConnection<K, V> connection, @NonNull BlockingQueue<V> queue, long pollingTimeout, @NonNull K[] patterns, @NonNull BiFunction<K,V,V> keyExtractor) {
        super(queue, pollingTimeout, patterns, keyExtractor);
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
}
