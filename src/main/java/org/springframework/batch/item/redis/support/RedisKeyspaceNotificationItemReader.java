package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class RedisKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V, StatefulRedisPubSubConnection<K, V>> {

    private RedisPubSubAdapter<K, V> listener;

    public RedisKeyspaceNotificationItemReader(Supplier<StatefulRedisPubSubConnection<K, V>> connectionSupplier, K pattern, Converter<K, K> keyExtractor, BlockingQueue<K> queue) {
        super(connectionSupplier, pattern, keyExtractor, queue);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void subscribe(StatefulRedisPubSubConnection<K, V> connection, K pattern) {
        listener = new RedisPubSubAdapter<K, V>() {

            @Override
            public void message(K channel, V message) {
                add(channel);
            }

            @Override
            public void message(K pattern, K channel, V message) {
                add(channel);
            }
        };
        log.debug("Adding listener");
        connection.addListener(listener);
        log.debug("Subscribing to channel pattern '{}'", pattern);
        connection.sync().psubscribe(pattern);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void unsubscribe(StatefulRedisPubSubConnection<K, V> connection, K pattern) {
        log.debug("Unsubscribing from channel pattern '{}'", pattern);
        connection.sync().punsubscribe(pattern);
        log.debug("Removing listener");
        connection.removeListener(listener);
    }

    public static RedisKeyspaceNotificationItemReaderBuilder client(RedisClient client) {
        return new RedisKeyspaceNotificationItemReaderBuilder(client);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisKeyspaceNotificationItemReaderBuilder extends KeyspaceNotificationItemReaderBuilder<RedisKeyspaceNotificationItemReaderBuilder> {

        private final RedisClient client;

        public RedisKeyspaceNotificationItemReaderBuilder(RedisClient client) {
            this.client = client;
        }

        public RedisKeyspaceNotificationItemReader<String, String> build() {
            return new RedisKeyspaceNotificationItemReader<>(client::connectPubSub, pattern(), STRING_KEY_EXTRACTOR, queue());
        }
    }

}