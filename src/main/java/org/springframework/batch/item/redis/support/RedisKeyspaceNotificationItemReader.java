package org.springframework.batch.item.redis.support;

import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class RedisKeyspaceNotificationItemReader extends AbstractKeyspaceNotificationItemReader<StatefulRedisPubSubConnection<String, String>> {

    private RedisPubSubAdapter<String, String> listener;

    public RedisKeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<String, String>> connectionSupplier, String pubSubPattern, int queueCapacity) {
        super(connectionSupplier, pubSubPattern, queueCapacity);
    }

    public RedisKeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<String, String>> connectionSupplier, String pubSubPattern, BlockingQueue<String> queue) {
        super(connectionSupplier, pubSubPattern, queue);
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
        log.info("Subscribing to channel pattern '{}'", pattern);
        connection.sync().psubscribe(pattern);
    }

    @Override
    protected void unsubscribe(StatefulRedisPubSubConnection<String, String> connection, String pattern) {
        log.info("Unsubscribing from channel pattern '{}'", pattern);
        connection.sync().punsubscribe(pattern);
        log.debug("Removing listener");
        connection.removeListener(listener);
    }

}