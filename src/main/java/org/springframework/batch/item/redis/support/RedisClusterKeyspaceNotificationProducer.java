package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.springframework.core.convert.converter.Converter;

public class RedisClusterKeyspaceNotificationProducer<K, V> extends AbstractKeyspaceNotificationProducer<K> implements RedisClusterPubSubListener<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;
    private final K pubSubPattern;

    public RedisClusterKeyspaceNotificationProducer(StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, K pubSubPattern, Converter<K, K> keyExtractor) {
        super(keyExtractor);
        this.pubSubConnection = pubSubConnection;
        this.pubSubPattern = pubSubPattern;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open() {
        pubSubConnection.addListener(this);
        pubSubConnection.setNodeMessagePropagation(true);
        pubSubConnection.sync().masters().commands().psubscribe(pubSubPattern);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void close() {
        pubSubConnection.sync().masters().commands().punsubscribe(pubSubPattern);
        pubSubConnection.removeListener(this);
    }

    @Override
    public void message(RedisClusterNode node, K channel, V message) {
        notification(channel);
    }

    @Override
    public void message(RedisClusterNode node, K pattern, K channel, V message) {
        notification(channel);
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
