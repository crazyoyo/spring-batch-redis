package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class RedisKeyEventItemReader extends AbstractKeyEventItemReader<StatefulRedisPubSubConnection<String, String>> {

    private RedisPubSubAdapter<String, String> listener;

    public RedisKeyEventItemReader(Supplier<StatefulRedisPubSubConnection<String, String>> connectionSupplier, int queueCapacity, String keyPattern) {
        super(connectionSupplier, queueCapacity, keyPattern);
    }

    @Override
    protected void subscribe(StatefulRedisPubSubConnection<String, String> connection, String pattern) {
        listener = new RedisPubSubAdapter<String, String>() {

            @Override
            public void message(String channel, String message) {
                add(channel);
            }

            @Override
            public void message(String pattern, String channel, String message) {
                add(channel);
            }
        };
        log.debug("Adding listener");
        connection.addListener(listener);
        log.debug("Subscribing to channel pattern '{}'", pattern);
        connection.sync().psubscribe(pattern);
    }

    @Override
    protected void unsubscribe(StatefulRedisPubSubConnection<String, String> connection, String pattern) {
        log.debug("Unsubscribing from channel pattern '{}'", pattern);
        connection.sync().punsubscribe(pattern);
        log.debug("Removing listener");
        connection.removeListener(listener);
    }

}