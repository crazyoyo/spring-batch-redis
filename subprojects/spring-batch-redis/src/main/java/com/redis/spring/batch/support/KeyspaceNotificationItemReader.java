package com.redis.spring.batch.support;

import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class KeyspaceNotificationItemReader extends AbstractKeyspaceNotificationItemReader<StatefulRedisPubSubConnection<String, String>> implements RedisPubSubListener<String, String> {

    public KeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, int queueCapacity) {
        super(connectionSupplier, pubSubPatterns, queueCapacity);
    }

    public KeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, BlockingQueue<String> queue) {
        super(connectionSupplier, pubSubPatterns, queue);
    }

    @Override
    protected void subscribe(StatefulRedisPubSubConnection<String, String> connection, List<String> patterns) {
        log.debug("Adding listener");
        connection.addListener(this);
        log.debug("Subscribing to channel patterns {}", patterns);
        connection.sync().psubscribe(patterns.toArray(new String[0]));
    }

    @Override
    protected void unsubscribe(StatefulRedisPubSubConnection<String, String> connection, List<String> patterns) {
        log.debug("Unsubscribing from channel pattern {}", patterns);
        connection.sync().punsubscribe(patterns.toArray(new String[0]));
        log.debug("Removing listener");
        connection.removeListener(this);
    }


    @Override
    public void message(String channel, String message) {
        add(channel);
    }

    @Override
    public void message(String pattern, String channel, String message) {
        add(channel);
    }

    @Override
    public void subscribed(String channel, long count) {
        // ignore
    }

    @Override
    public void unsubscribed(String channel, long count) {
        // ignore
    }

    @Override
    public void psubscribed(String pattern, long count) {
        // ignore
    }

    @Override
    public void punsubscribed(String pattern, long count) {
        // empty adapter method
    }

}
