package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.AbstractLiveKeyItemReader;
import org.springframework.batch.item.redis.support.LiveKeyReaderOptions;
import org.springframework.util.Assert;

public class RedisLiveKeyItemReader<K, V> extends AbstractLiveKeyItemReader<K, V, StatefulRedisConnection<K, V>> implements RedisPubSubListener<K, V> {

    private final StatefulRedisPubSubConnection<K, V> pubSubConnection;

    public RedisLiveKeyItemReader(StatefulRedisConnection<K, V> connection, StatefulRedisPubSubConnection<K, V> pubSubConnection, LiveKeyReaderOptions<K> options) {
        super(connection, StatefulRedisConnection::sync, options);
        this.pubSubConnection = pubSubConnection;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void open(K channelPattern) {
        pubSubConnection.addListener(this);
        pubSubConnection.sync().psubscribe(channelPattern);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void close(K channelPattern) {
        pubSubConnection.sync().punsubscribe(channelPattern);
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

    public static RedisLiveKeyItemReaderBuilder builder() {
        return new RedisLiveKeyItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisLiveKeyItemReaderBuilder {

        private RedisClient client;
        private LiveKeyReaderOptions<String> options = LiveKeyReaderOptions.builder().build();

        public RedisLiveKeyItemReader<String, String> build() {
            Assert.notNull(client, "A RedisClient is required.");
            Assert.notNull(options, "Options are required.");
            return new RedisLiveKeyItemReader<>(client.connect(), client.connectPubSub(), options);
        }

    }
}
