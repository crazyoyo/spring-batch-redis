package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

@Slf4j
public class RedisClusterKeyspaceNotificationItemReader extends AbstractKeyspaceNotificationItemReader<StatefulRedisClusterPubSubConnection<String, String>> implements RedisClusterPubSubListener<String, String> {

    public RedisClusterKeyspaceNotificationItemReader(Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, int queueCapacity) {
        super(connectionSupplier, pubSubPatterns, queueCapacity);
    }

    public RedisClusterKeyspaceNotificationItemReader(Supplier<StatefulRedisClusterPubSubConnection<String, String>> connectionSupplier, List<String> pubSubPatterns, BlockingQueue<String> queue) {
        super(connectionSupplier, pubSubPatterns, queue);
    }

    @Override
    protected void subscribe(StatefulRedisClusterPubSubConnection<String, String> connection, List<String> patterns) {
        log.debug("Adding listener");
        connection.addListener(this);
        connection.setNodeMessagePropagation(true);
        log.debug("Subscribing to channel patterns {}", patterns);
        connection.sync().upstream().commands().psubscribe(patterns.toArray(new String[0]));
    }

    @Override
    public void message(RedisClusterNode node, String channel, String message) {
        add(channel);
    }

    @Override
    public void message(RedisClusterNode node, String pattern, String channel, String message) {
        add(channel);
    }

    @Override
    public void subscribed(RedisClusterNode node, String channel, long count) {
        // ignore
    }

    @Override
    public void psubscribed(RedisClusterNode node, String pattern, long count) {
        // ignore
    }

    @Override
    public void unsubscribed(RedisClusterNode node, String channel, long count) {
        // ignore
    }

    @Override
    public void punsubscribed(RedisClusterNode node, String pattern, long count) {
        // ignore
    }

    @Override
    protected void unsubscribe(StatefulRedisClusterPubSubConnection<String, String> connection, List<String> patterns) {
        log.debug("Unsubscribing from channel patterns {}", patterns);
        connection.sync().upstream().commands().punsubscribe(patterns.toArray(new String[0]));
        log.debug("Removing listener");
        connection.removeListener(this);
    }

}
