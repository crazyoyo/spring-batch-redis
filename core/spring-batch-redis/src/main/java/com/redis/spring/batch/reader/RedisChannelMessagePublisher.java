package com.redis.spring.batch.reader;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisChannelMessagePublisher extends AbstractKeyspaceNotificationPublisher
        implements RedisPubSubListener<String, String> {

    private final RedisClient client;

    private StatefulRedisPubSubConnection<String, String> connection;

    public RedisChannelMessagePublisher(RedisClient client) {
        this.client = client;
    }

    @Override
    protected void subscribe(String... patterns) {
        connection = client.connectPubSub();
        connection.addListener(this);
        connection.sync().psubscribe(patterns);
    }

    @Override
    protected void unsubscribe(String... patterns) {
        connection.sync().punsubscribe(patterns);
        connection.removeListener(this);
    }

    @Override
    public void message(String channel, String message) {
        // ignore
    }

    @Override
    public void message(String pattern, String channel, String message) {
        channelMessage(pattern, channel, message);
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
