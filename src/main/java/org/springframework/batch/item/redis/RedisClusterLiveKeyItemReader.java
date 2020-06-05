package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import lombok.Builder;
import org.springframework.batch.item.redis.support.AbstractLiveKeyItemReader;
import org.springframework.core.convert.converter.Converter;

public class RedisClusterLiveKeyItemReader<K, V> extends AbstractLiveKeyItemReader<K, V, StatefulRedisClusterConnection<K, V>> implements RedisClusterPubSubListener<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;

    public RedisClusterLiveKeyItemReader(StatefulRedisClusterConnection<K, V> connection, Long scanCount, String scanPattern, StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, K[] patterns, Integer queueCapacity, Long queuePollingTimeout, Converter<K, K> channelKeyConverter) {
        super(connection, StatefulRedisClusterConnection::sync, scanCount, scanPattern, patterns, queueCapacity, queuePollingTimeout, channelKeyConverter);
        this.pubSubConnection = pubSubConnection;
    }

    @Override
    protected void open(K[] patterns) {
        pubSubConnection.addListener(this);
        pubSubConnection.setNodeMessagePropagation(true);
        pubSubConnection.sync().masters().commands().psubscribe(patterns);
    }

    @Override
    protected void close(K[] patterns) {
        pubSubConnection.sync().masters().commands().punsubscribe(patterns);
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

    @Builder
    private static RedisClusterLiveKeyItemReader<String, String> build(RedisClusterClient redisClusterClient, int database, Long scanCount, String scanPattern, String[] pubSubPatterns, Integer queueCapacity, Long queuePollingTimeout) {
        return new RedisClusterLiveKeyItemReader<>(redisClusterClient.connect(), scanCount, scanPattern, redisClusterClient.connectPubSub(), channels(database, scanPattern, pubSubPatterns), queueCapacity, queuePollingTimeout, DEFAULT_CHANNEL_KEY_CONVERTER);
    }

}
