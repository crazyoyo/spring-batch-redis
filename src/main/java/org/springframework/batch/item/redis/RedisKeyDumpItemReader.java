package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDumpItemReader;
import org.springframework.batch.item.redis.support.LiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.ScanKeyValueItemReaderBuilder;

import java.util.function.Function;

public class RedisKeyDumpItemReader<K, V> extends KeyDumpItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisKeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }


    public static ScanKeyValueItemReaderBuilder<RedisKeyDumpItemReader<String, String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new ScanKeyValueItemReaderBuilder<RedisKeyDumpItemReader<String, String>>() {

            @Override
            protected RedisKeyDumpItemReader<String, String> build(int chunkSize, int threadCount, int queueCapacity) {
                return new RedisKeyDumpItemReader<>(keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
            }
        };
    }

    public static LiveKeyValueItemReaderBuilder<RedisKeyDumpItemReader<String, String>> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new LiveKeyValueItemReaderBuilder<RedisKeyDumpItemReader<String, String>>() {

            @Override
            protected RedisKeyDumpItemReader<String, String> build(int chunkSize, int threadCount, int queueCapacity, Function<SimpleStepBuilder<String, String>, SimpleStepBuilder<String, String>> stepBuilderProvider) {
                return new RedisKeyDumpItemReader<>(keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider);
            }
        };
    }
}
