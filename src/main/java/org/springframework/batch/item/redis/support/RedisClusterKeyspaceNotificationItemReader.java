package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class RedisClusterKeyspaceNotificationItemReader extends AbstractKeyspaceNotificationItemReader<StatefulRedisClusterPubSubConnection<String, String>> {

    private RedisClusterPubSubListener<String, String> listener;

    public RedisClusterKeyspaceNotificationItemReader(Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, int queueCapacity) {
        super(connectionSupplier, pubSubPatterns, queueCapacity);
    }

    public RedisClusterKeyspaceNotificationItemReader(Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, BlockingQueue<String> queue) {
        super(connectionSupplier, pubSubPatterns, queue);
    }

    @Override
    protected void subscribe(StatefulRedisClusterPubSubConnection<String, String> connection, List<String> patterns) {
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
        log.debug("Subscribing to channel patterns {}", patterns);
        connection.sync().upstream().commands().psubscribe(patterns.toArray(new String[0]));
    }

    @Override
    protected void unsubscribe(StatefulRedisClusterPubSubConnection<String, String> connection, List<String> patterns) {
        log.debug("Unsubscribing from channel patterns {}", patterns);
        connection.sync().upstream().commands().punsubscribe(patterns.toArray(new String[0]));
        log.debug("Removing listener");
        connection.removeListener(listener);
    }

}
