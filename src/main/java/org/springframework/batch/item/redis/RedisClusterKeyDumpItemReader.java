package org.springframework.batch.item.redis;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.*;
import org.springframework.util.Assert;

import java.time.Duration;

public class RedisClusterKeyDumpItemReader<K, V> extends AbstractKeyDumpItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public RedisClusterKeyDumpItemReader(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, ItemReader<K> keyReader, Duration commandTimeout, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, commandTimeout, chunkSize, threads, queueCapacity);
        Assert.notNull(pool, "A connection pool is required.");
        this.pool = pool;
    }

    @Override
    protected StatefulRedisClusterConnection<K, V> connection() throws Exception {
        return pool.borrowObject();
    }

    @Override
    protected BaseRedisAsyncCommands<K, V> commands(StatefulRedisClusterConnection<K, V> connection) {
        return connection.async();
    }

    public static RedisClusterKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterKeyDumpItemReaderBuilder(pool, connection);
    }

    public static RedisClusterNotificationKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new RedisClusterNotificationKeyDumpItemReaderBuilder(pool, connection);
    }

    public static class RedisClusterKeyDumpItemReaderBuilder extends ScanKeyValueItemReaderBuilder<RedisClusterKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterConnection<String, String> connection;

        public RedisClusterKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisClusterKeyDumpItemReader<String, String> build() {
            RedisClusterKeyItemReader<String, String> keyReader = new RedisClusterKeyItemReader<>(connection, commandTimeout, scanCount, keyPattern, sampleSize, keyPatternPredicate());
            return new RedisClusterKeyDumpItemReader<>(pool, keyReader, commandTimeout, chunkSize, threadCount, queueCapacity);
        }

    }

    public static class RedisClusterNotificationKeyDumpItemReaderBuilder extends NotificationKeyValueItemReaderBuilder<RedisClusterNotificationKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterPubSubConnection<String, String> connection;

        public RedisClusterNotificationKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisClusterKeyDumpItemReader<String, String> build() {
            RedisClusterKeyspaceNotificationItemReader<String, String> keyReader = new RedisClusterKeyspaceNotificationItemReader<>(connection, pubSubPattern(), DEFAULT_KEY_EXTRACTOR, notificationQueueCapacity);
            return new RedisClusterKeyDumpItemReader<>(pool, keyReader, commandTimeout, chunkSize, threadCount, queueCapacity);
        }

    }
}
