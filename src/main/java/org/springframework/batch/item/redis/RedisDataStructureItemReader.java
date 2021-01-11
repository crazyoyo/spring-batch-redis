package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;
import org.springframework.util.Assert;

import java.time.Duration;

public class RedisDataStructureItemReader<K, V> extends AbstractDataStructureItemReader<K, V, StatefulRedisConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public RedisDataStructureItemReader(GenericObjectPool<StatefulRedisConnection<K, V>> pool, ItemReader<K> keyReader, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Duration pollingTimeout) {
        super(keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
        Assert.notNull(pool, "A connection pool is required.");
        this.pool = pool;
    }

    @Override
    protected StatefulRedisConnection<K, V> connection() throws Exception {
        return pool.borrowObject();
    }

    @Override
    protected BaseRedisAsyncCommands<K, V> commands(StatefulRedisConnection<K, V> connection) {
        return connection.async();
    }

    public static RedisDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new RedisDataStructureItemReaderBuilder(pool, connection);
    }

    public static RedisNotificationDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new RedisNotificationDataStructureItemReaderBuilder(pool, connection);
    }

    public static class RedisDataStructureItemReaderBuilder extends ScanKeyValueItemReaderBuilder<RedisDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisConnection<String, String> connection;

        public RedisDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisDataStructureItemReader<String, String> build() {
            RedisKeyItemReader<String, String> keyReader = new RedisKeyItemReader<>(connection, commandTimeout, scanCount, keyPattern);
            return new RedisDataStructureItemReader<>(pool, keyReader, commandTimeout, chunkSize, threadCount, queueCapacity, queuePollingTimeout);
        }

    }

    public static class RedisNotificationDataStructureItemReaderBuilder extends NotificationKeyValueItemReaderBuilder<RedisNotificationDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisPubSubConnection<String, String> connection;

        public RedisNotificationDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisDataStructureItemReader<String, String> build() {
            RedisKeyspaceNotificationItemReader<String, String> keyReader = new RedisKeyspaceNotificationItemReader<>(connection, pubSubPattern(), DEFAULT_KEY_EXTRACTOR, notificationQueueCapacity, notificationQueuePollingTimeout);
            return new RedisDataStructureItemReader<>(pool, keyReader, commandTimeout, chunkSize, threadCount, queueCapacity, queuePollingTimeout);
        }

    }
}
