package com.redis.spring.batch.reader;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterKeyspaceNotificationPublisher extends AbstractKeyspaceNotificationPublisher
        implements RedisClusterPubSubListener<String, String> {

    private final RedisClusterClient client;

    private final String pattern;

    private StatefulRedisClusterPubSubConnection<String, String> connection;

    public RedisClusterKeyspaceNotificationPublisher(RedisClusterClient client, String pattern) {
        this.client = client;
        this.pattern = pattern;
    }

    @Override
    public void open() {
        connection = client.connectPubSub();
        connection.setNodeMessagePropagation(true);
        connection.addListener(this);
        connection.sync().upstream().commands().psubscribe(pattern);
    }

    @Override
    public void close() {
        connection.sync().upstream().commands().punsubscribe(pattern);
        connection.removeListener(this);
        connection.close();
    }

    @Override
    public void message(RedisClusterNode node, String channel, String message) {
        // ignore
    }

    @Override
    public void message(RedisClusterNode node, String pattern, String channel, String message) {
        notification(channel, message);
    }

    @Override
    public void subscribed(RedisClusterNode node, String channel, long count) {
        // ignore
    }

    @Override
    public void psubscribed(RedisClusterNode node, String pattern, long count) {
        // ignore
    }

    @Override
    public void unsubscribed(RedisClusterNode node, String channel, long count) {
        // ignore
    }

    @Override
    public void punsubscribed(RedisClusterNode node, String pattern, long count) {
        // ignore
    }

}
