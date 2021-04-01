package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructureItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;

import java.time.Duration;
import java.util.function.Function;

public class RedisClusterDataStructureItemReader<K, V> extends DataStructureItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterDataStructureItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(readTimeout, keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisClusterDataStructureItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(readTimeout, keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    public static RedisClusterScanDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterScanDataStructureItemReaderBuilder(pool, connection);
    }

    public static class RedisClusterScanDataStructureItemReaderBuilder extends ScanKeyValueItemReaderBuilder<RedisClusterScanDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterConnection<String, String> connection;

        public RedisClusterScanDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisClusterDataStructureItemReader<String, String> build() {
            return new RedisClusterDataStructureItemReader<>(readTimeout, keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
        }
    }

    public static RedisClusterLiveDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new RedisClusterLiveDataStructureItemReaderBuilder(pool, connection);
    }

    public static class RedisClusterLiveDataStructureItemReaderBuilder extends LiveKeyValueItemReaderBuilder<RedisClusterLiveDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private final StatefulRedisClusterPubSubConnection<String, String> connection;

        public RedisClusterLiveDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        public RedisClusterDataStructureItemReader<String, String> build() {
            return new RedisClusterDataStructureItemReader<>(readTimeout, keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
        }
    }

}
