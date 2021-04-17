package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDumpItemReader;
import org.springframework.batch.item.redis.support.AbstractLiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.AbstractScanKeyValueItemReaderBuilder;

import java.time.Duration;
import java.util.function.Function;

public class RedisClusterKeyDumpItemReader<K, V> extends KeyDumpItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterKeyDumpItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(readTimeout, keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisClusterKeyDumpItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(readTimeout, keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }


    public static RedisClusterScanKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterScanKeyDumpItemReaderBuilder(pool, connection);
    }

    public static class RedisClusterScanKeyDumpItemReaderBuilder extends AbstractScanKeyValueItemReaderBuilder<RedisClusterKeyDumpItemReader<String, String>, RedisClusterScanKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterConnection<String, String> connection;

        public RedisClusterScanKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        @Override
        public RedisClusterKeyDumpItemReader<String, String> build() {
            return new RedisClusterKeyDumpItemReader<>(readTimeout, keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
        }
    }

    public static RedisClusterLiveKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new RedisClusterLiveKeyDumpItemReaderBuilder(pool, connection);
    }

    public static class RedisClusterLiveKeyDumpItemReaderBuilder extends AbstractLiveKeyValueItemReaderBuilder<RedisClusterKeyDumpItemReader<String, String>, RedisClusterLiveKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterPubSubConnection<String, String> connection;

        public RedisClusterLiveKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        @Override
        public RedisClusterKeyDumpItemReader<String, String> build() {
            return new RedisClusterKeyDumpItemReader<>(readTimeout, keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
        }
    }
}
