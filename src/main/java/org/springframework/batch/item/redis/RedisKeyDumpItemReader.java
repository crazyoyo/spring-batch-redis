package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;

import java.time.Duration;

public class RedisKeyDumpItemReader<K, V> extends AbstractKeyDumpItemReader<K, V, StatefulRedisConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public RedisKeyDumpItemReader(GenericObjectPool<StatefulRedisConnection<K, V>> pool, ItemReader<K> keyReader, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Duration pollingTimeout) {
        super(keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
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

    public static RedisKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new RedisKeyDumpItemReaderBuilder(pool, connection);
    }

    public static RedisNotificationKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new RedisNotificationKeyDumpItemReaderBuilder(pool, connection);
    }

    public static class RedisKeyDumpItemReaderBuilder extends ScanKeyValueItemReaderBuilder<RedisKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisConnection<String, String> connection;

        public RedisKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisKeyDumpItemReader<String, String> build() {
            RedisKeyItemReader<String, String> keyReader = new RedisKeyItemReader<>(connection, commandTimeout, scanCount, keyPattern);
            return new RedisKeyDumpItemReader<>(pool, keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
        }

    }

    public static class RedisNotificationKeyDumpItemReaderBuilder extends NotificationKeyValueItemReaderBuilder<RedisNotificationKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisPubSubConnection<String,String> connection;

        public RedisNotificationKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisKeyDumpItemReader<String, String> build() {
            RedisKeyspaceNotificationItemReader<String, String> keyReader = new RedisKeyspaceNotificationItemReader<>(connection, pubSubPattern(), DEFAULT_KEY_EXTRACTOR, queueCapacity, queuePollingTimeout);
            return new RedisKeyDumpItemReader<>(pool, keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
        }

    }
}
