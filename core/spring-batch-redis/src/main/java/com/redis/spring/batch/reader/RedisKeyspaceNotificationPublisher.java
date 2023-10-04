package com.redis.spring.batch.reader;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisKeyspaceNotificationPublisher extends AbstractKeyspaceNotificationPublisher
        implements RedisPubSubListener<String, String> {

    private final RedisClient client;

    private final String pattern;

    private StatefulRedisPubSubConnection<String, String> connection;

    public RedisKeyspaceNotificationPublisher(RedisClient client, String pattern) {
        this.client = client;
        this.pattern = pattern;
    }

    @Override
    public void open() {
        connection = client.connectPubSub();
        connection.addListener(this);
        connection.sync().psubscribe(pattern);
    }

    @Override
    public void close() {
        connection.sync().punsubscribe(pattern);
        connection.removeListener(this);
        connection.close();
    }

    @Override
    public void message(String channel, String message) {
        // ignore
    }

    @Override
    public void message(String pattern, String channel, String message) {
        notification(channel, message);
    }

    @Override
    public void subscribed(String channel, long count) {
        // ignore
    }

    @Override
    public void psubscribed(String pattern, long count) {
        // ignore
    }

    @Override
    public void unsubscribed(String channel, long count) {
        // ignore
    }

    @Override
    public void punsubscribed(String pattern, long count) {
        // ignore
    }

}
