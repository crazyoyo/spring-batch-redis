package org.springframework.batch.item.redis;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemReader;
import org.springframework.batch.item.redis.support.RedisClusterKeyItemReader;
import org.springframework.batch.item.redis.support.RedisClusterKeyspaceNotificationItemReader;
import org.springframework.util.Assert;

import java.time.Duration;

public class RedisClusterDataStructureItemReader<K, V> extends AbstractDataStructureItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public RedisClusterDataStructureItemReader(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, ItemReader<K> keyReader, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Duration pollingTimeout) {
        super(keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
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

    public static RedisClusterDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterDataStructureItemReaderBuilder(pool, connection);
    }

    public static RedisClusterNotificationDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new RedisClusterNotificationDataStructureItemReaderBuilder(pool, connection);
    }

    public static class RedisClusterDataStructureItemReaderBuilder extends ScanKeyValueItemReaderBuilder<RedisClusterDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterConnection<String, String> connection;

        public RedisClusterDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisClusterDataStructureItemReader<String, String> build() {
            RedisClusterKeyItemReader<String, String> keyReader = new RedisClusterKeyItemReader<>(connection, commandTimeout, scanCount, keyPattern);
            return new RedisClusterDataStructureItemReader<>(pool, keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
        }

    }

    public static class RedisClusterNotificationDataStructureItemReaderBuilder extends NotificationKeyValueItemReaderBuilder<RedisClusterNotificationDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterPubSubConnection<String,String> connection;

        public RedisClusterNotificationDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisClusterDataStructureItemReader<String, String> build() {
            RedisClusterKeyspaceNotificationItemReader<String, String> keyReader = new RedisClusterKeyspaceNotificationItemReader<>(connection, pubSubPattern(), DEFAULT_KEY_EXTRACTOR, queueCapacity, queuePollingTimeout);
            return new RedisClusterDataStructureItemReader<>(pool, keyReader, commandTimeout, chunkSize, threads, queueCapacity, pollingTimeout);
        }

    }
}
