package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Builder;
import org.springframework.batch.item.redis.support.AbstractLiveKeyItemReader;
import org.springframework.core.convert.converter.Converter;

public class RedisLiveKeyItemReader<K, V> extends AbstractLiveKeyItemReader<K, V, StatefulRedisConnection<K, V>> implements RedisPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> pubSubConnection;

    public RedisLiveKeyItemReader(StatefulRedisConnection<K, V> connection, Long scanCount, String scanPattern, StatefulRedisPubSubConnection<K, V> pubSubConnection, K[] patterns, Integer queueCapacity, Long queuePollingTimeout, Converter<K, K> channelKeyConverter) {
        super(connection, StatefulRedisConnection::sync, scanCount, scanPattern, patterns, queueCapacity, queuePollingTimeout, channelKeyConverter);
        this.pubSubConnection = pubSubConnection;
    }

    @Override
    protected void open(K[] patterns) {
        pubSubConnection.addListener(this);
        pubSubConnection.sync().psubscribe(patterns);
    }

    @Override
    protected void close(K[] patterns) {
        pubSubConnection.sync().punsubscribe(patterns);
        pubSubConnection.removeListener(this);
    }

    @Override
    public void message(K channel, V message) {
        enqueue(channel);
    }

    @Override
    public void message(K pattern, K channel, V message) {
        message(channel, message);
    }

    @Override
    public void subscribed(K channel, long count) {
    }

    @Override
    public void psubscribed(K pattern, long count) {
    }

    @Override
    public void unsubscribed(K channel, long count) {
    }

    @Override
    public void punsubscribed(K pattern, long count) {
    }

    @Builder
    private static RedisLiveKeyItemReader<String, String> build(RedisClient redisClient, int database, Long scanCount, String scanPattern, String[] pubSubPatterns, Integer queueCapacity, Long queuePollingTimeout) {
        return new RedisLiveKeyItemReader<>(redisClient.connect(), scanCount, scanPattern, redisClient.connectPubSub(), channels(database, scanPattern, pubSubPatterns), queueCapacity, queuePollingTimeout, DEFAULT_CHANNEL_KEY_CONVERTER);
    }

}
