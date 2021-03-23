package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructureItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;

import java.util.function.Function;

public class RedisDataStructureItemReader<K, V> extends DataStructureItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisDataStructureItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisDataStructureItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    public static ScanKeyValueItemReaderBuilder<RedisDataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder<RedisDataStructureItemReader<String, String>>() {
            @Override
            public RedisDataStructureItemReader<String, String> build() {
                return new RedisDataStructureItemReader<>(keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<RedisDataStructureItemReader<String, String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder<RedisDataStructureItemReader<String, String>>() {
            @Override
            public RedisDataStructureItemReader<String, String> build() {
                return new RedisDataStructureItemReader<>(keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
            }
        };
    }

}
