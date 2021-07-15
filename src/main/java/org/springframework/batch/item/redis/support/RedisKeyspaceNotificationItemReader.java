package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class RedisKeyspaceNotificationItemReader extends AbstractKeyspaceNotificationItemReader<StatefulRedisPubSubConnection<String, String>> {

    private RedisPubSubAdapter<String, String> listener;

    public RedisKeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, int queueCapacity) {
        super(connectionSupplier, pubSubPatterns, queueCapacity);
    }

    public RedisKeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, BlockingQueue<String> queue) {
        super(connectionSupplier, pubSubPatterns, queue);
    }

    @Override
    protected void subscribe(StatefulRedisPubSubConnection<String, String> connection, List<String> patterns) {
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
        log.debug("Subscribing to channel patterns {}", patterns);
        connection.sync().psubscribe(patterns.toArray(new String[0]));
    }

    @Override
    protected void unsubscribe(StatefulRedisPubSubConnection<String, String> connection, List<String> patterns) {
        log.debug("Unsubscribing from channel pattern {}", patterns);
        connection.sync().punsubscribe(patterns.toArray(new String[0]));
        log.debug("Removing listener");
        connection.removeListener(listener);
    }

}
