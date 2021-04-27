package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class RedisClusterKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V, StatefulRedisClusterPubSubConnection<K, V>> {

    private RedisClusterPubSubListener<K, V> listener;

    public RedisClusterKeyspaceNotificationItemReader(Supplier<StatefulRedisClusterPubSubConnection<K, V>> connectionSupplier, K pattern, Converter<K, K> keyExtractor, BlockingQueue<K> queue) {
        super(connectionSupplier, pattern, keyExtractor, queue);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void subscribe(StatefulRedisClusterPubSubConnection<K, V> connection, K pattern) {
        listener = new RedisClusterPubSubAdapter<K, V>() {

            @Override
            public void message(RedisClusterNode node, K channel, V message) {
                add(channel);
            }

            @Override
            public void message(RedisClusterNode node, K pattern, K channel, V message) {
                add(channel);
            }

        };
        log.debug("Adding listener");
        connection.addListener(listener);
        connection.setNodeMessagePropagation(true);
        log.debug("Subscribing to channel pattern '{}'", pattern);
        connection.sync().upstream().commands().psubscribe(pattern);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void unsubscribe(StatefulRedisClusterPubSubConnection<K, V> connection, K pattern) {
        log.debug("Unsubscribing from channel pattern '{}'", pattern);
        connection.sync().upstream().commands().punsubscribe(pattern);
        log.debug("Removing listener");
        connection.removeListener(listener);
    }

    public static RedisClusterKeyspaceNotificationItemReaderBuilder client(RedisClusterClient client) {
        return new RedisClusterKeyspaceNotificationItemReaderBuilder(client);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisClusterKeyspaceNotificationItemReaderBuilder extends KeyspaceNotificationItemReaderBuilder<RedisClusterKeyspaceNotificationItemReaderBuilder> {

        private final RedisClusterClient client;

        public RedisClusterKeyspaceNotificationItemReaderBuilder(RedisClusterClient client) {
            this.client = client;
        }

        public RedisClusterKeyspaceNotificationItemReader<String, String> build() {
            return new RedisClusterKeyspaceNotificationItemReader<>(client::connectPubSub, pattern(), STRING_KEY_EXTRACTOR, queue());
        }
    }

}