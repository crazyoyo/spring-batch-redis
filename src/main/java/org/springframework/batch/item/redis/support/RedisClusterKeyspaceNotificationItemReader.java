package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class RedisClusterKeyspaceNotificationItemReader extends AbstractKeyspaceNotificationItemReader<StatefulRedisClusterPubSubConnection<String, String>> {

    private RedisClusterPubSubListener<String, String> listener;

    public RedisClusterKeyspaceNotificationItemReader(Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier, String pubSubPattern, int queueCapacity) {
        super(connectionSupplier, pubSubPattern, queueCapacity);
    }

    public RedisClusterKeyspaceNotificationItemReader(Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier, String pubSubPattern, BlockingQueue<String> queue) {
        super(connectionSupplier, pubSubPattern, queue);
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