package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.AbstractLiveKeyItemReader;
import org.springframework.batch.item.redis.support.LiveKeyReaderOptions;
import org.springframework.util.Assert;

public class RedisClusterLiveKeyItemReader<K, V> extends AbstractLiveKeyItemReader<K, V, StatefulRedisClusterConnection<K, V>> implements RedisClusterPubSubListener<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;

    public RedisClusterLiveKeyItemReader(StatefulRedisClusterConnection<K, V> connection, StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, LiveKeyReaderOptions<K> options) {
        super(connection, StatefulRedisClusterConnection::sync, options);
        this.pubSubConnection = pubSubConnection;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void open(K pubsubPattern) {
        pubSubConnection.addListener(this);
        pubSubConnection.setNodeMessagePropagation(true);
        pubSubConnection.sync().masters().commands().psubscribe(pubsubPattern);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void close(K pubsubPattern) {
        pubSubConnection.sync().masters().commands().punsubscribe(pubsubPattern);
        pubSubConnection.removeListener(this);
    }

    @Override
    public void message(RedisClusterNode node, K channel, V message) {
        enqueue(channel);
    }

    @Override
    public void message(RedisClusterNode node, K pattern, K channel, V message) {
        enqueue(channel);
    }

    @Override
    public void subscribed(RedisClusterNode node, K channel, long count) {
    }

    @Override
    public void psubscribed(RedisClusterNode node, K pattern, long count) {
    }

    @Override
    public void unsubscribed(RedisClusterNode node, K channel, long count) {
    }

    @Override
    public void punsubscribed(RedisClusterNode node, K pattern, long count) {
    }

    public static RedisClusterLiveKeyItemReaderBuilder builder() {
        return new RedisClusterLiveKeyItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisClusterLiveKeyItemReaderBuilder {

        private RedisClusterClient client;
        private LiveKeyReaderOptions<String> options = LiveKeyReaderOptions.builder().build();

        public RedisClusterLiveKeyItemReader<String, String> build() {
            Assert.notNull(client, "A RedisClusterClient is required.");
            Assert.notNull(options, "Options are required.");
            return new RedisClusterLiveKeyItemReader<>(client.connect(), client.connectPubSub(), options);
        }

    }

}
