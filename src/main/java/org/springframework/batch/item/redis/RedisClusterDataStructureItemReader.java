package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructureItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;

import java.util.function.Function;

public class RedisClusterDataStructureItemReader<K, V> extends DataStructureItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterDataStructureItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisClusterDataStructureItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    public static ScanKeyValueItemReaderBuilder<RedisClusterDataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder<RedisClusterDataStructureItemReader<String, String>>() {

            @Override
            protected RedisClusterDataStructureItemReader<String, String> build(int chunkSize, int threadCount, int queueCapacity) {
                return new RedisClusterDataStructureItemReader<>(keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<RedisClusterDataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder<RedisClusterDataStructureItemReader<String, String>>() {

            @Override
            protected RedisClusterDataStructureItemReader<String, String> build(int chunkSize, int threadCount, int queueCapacity, Function<SimpleStepBuilder<String, String>, SimpleStepBuilder<String, String>> stepBuilderProvider) {
                return new RedisClusterDataStructureItemReader<>(keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider);
            }
        };
    }

}
