package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDumpItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;

import java.util.function.Function;

public class RedisClusterKeyDumpItemReader<K, V> extends KeyDumpItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterKeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisClusterKeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(keyReader, pool, StatefulRedisClusterConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }


    public static ScanKeyValueItemReaderBuilder<RedisClusterKeyDumpItemReader<String, String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder<RedisClusterKeyDumpItemReader<String, String>>() {
            @Override
            public RedisClusterKeyDumpItemReader<String, String> build() {
                return new RedisClusterKeyDumpItemReader<>(keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<RedisClusterKeyDumpItemReader<String, String>> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder<RedisClusterKeyDumpItemReader<String, String>>() {
            @Override
            public RedisClusterKeyDumpItemReader<String, String> build() {
                return new RedisClusterKeyDumpItemReader<>(keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
            }
        };
    }
}
