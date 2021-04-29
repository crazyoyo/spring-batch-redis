package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class RedisClusterKeyEventItemReader extends AbstractKeyEventItemReader<StatefulRedisClusterPubSubConnection<String, String>> {

    private RedisClusterPubSubListener<String, String> listener;

    public RedisClusterKeyEventItemReader(Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier, int queueCapacity, String keyPattern) {
        super(connectionSupplier, queueCapacity, keyPattern);
    }

    @Override
    protected void subscribe(StatefulRedisClusterPubSubConnection<String, String> connection, String pattern) {
        listener = new RedisClusterPubSubAdapter<String, String>() {

            @Override
            public void message(RedisClusterNode node, String channel, String message) {
                add(channel);
            }

            @Override
            public void message(RedisClusterNode node, String pattern, String channel, String message) {
                add(channel);
            }

        };
        log.debug("Adding listener");
        connection.addListener(listener);
        connection.setNodeMessagePropagation(true);
        log.debug("Subscribing to channel pattern '{}'", pattern);
        connection.sync().upstream().commands().psubscribe(pattern);
    }

    @Override
    protected void unsubscribe(StatefulRedisClusterPubSubConnection<String, String> connection, String pattern) {
        log.debug("Unsubscribing from channel pattern '{}'", pattern);
        connection.sync().upstream().commands().punsubscribe(pattern);
        log.debug("Removing listener");
        connection.removeListener(listener);
    }

}