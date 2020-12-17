package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;

public class RedisClusterKeyspaceNotificationItemReader<K, V> extends AbstractKeyspaceNotificationItemReader<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> connection;
    private final ClusterKeyspaceNotificationListener listener = new ClusterKeyspaceNotificationListener();

    protected RedisClusterKeyspaceNotificationItemReader(StatefulRedisClusterPubSubConnection<K, V> connection, KeyspaceNotificationReaderOptions<K> options) {
        super(options);
        this.connection = connection;
    }

    @Override
    protected void subscribe(K pattern) {
        connection.addListener(listener);
        connection.setNodeMessagePropagation(true);
        connection.sync().upstream().commands().psubscribe(pattern);
    }

    @Override
    protected void unsubscribe(K pubSubPattern) {
        connection.sync().upstream().commands().punsubscribe(pubSubPattern);
        connection.removeListener(listener);
    }

    private class ClusterKeyspaceNotificationListener extends RedisClusterPubSubAdapter<K, V> {

        @Override
        public void message(RedisClusterNode node, K channel, V message) {
            notification(channel, message);
        }

        @Override
        public void message(RedisClusterNode node, K pattern, K channel, V message) {
            notification(channel, message);
        }
    }
}
