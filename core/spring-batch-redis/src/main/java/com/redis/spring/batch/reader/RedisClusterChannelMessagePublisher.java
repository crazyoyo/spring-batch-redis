package com.redis.spring.batch.reader;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterChannelMessagePublisher extends AbstractKeyspaceNotificationPublisher
        implements RedisClusterPubSubListener<String, String> {

    private final RedisClusterClient client;

    private StatefulRedisClusterPubSubConnection<String, String> connection;

    public RedisClusterChannelMessagePublisher(RedisClusterClient client) {
        this.client = client;
    }

    @Override
    protected void subscribe(String... patterns) {
        connection = client.connectPubSub();
        connection.setNodeMessagePropagation(true);
        connection.addListener(this);
        connection.sync().upstream().commands().psubscribe(patterns);
    }

    @Override
    protected void unsubscribe(String... patterns) {
        connection.sync().upstream().commands().punsubscribe(patterns);
        connection.removeListener(this);
    }

    @Override
    public void message(RedisClusterNode node, String channel, String message) {
        // ignore
    }

    @Override
    public void message(RedisClusterNode node, String pattern, String channel, String message) {
        channelMessage(pattern, channel, message);
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
