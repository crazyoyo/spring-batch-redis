package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDumpItemReader;
import org.springframework.batch.item.redis.support.AbstractLiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.AbstractScanKeyValueItemReaderBuilder;

import java.time.Duration;
import java.util.function.Function;

public class RedisKeyDumpItemReader<K, V> extends KeyDumpItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyDumpItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(readTimeout, keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisKeyDumpItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(readTimeout, keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    public static RedisScanKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new RedisScanKeyDumpItemReaderBuilder(pool, connection);
    }

    public static class RedisScanKeyDumpItemReaderBuilder extends AbstractScanKeyValueItemReaderBuilder<RedisKeyDumpItemReader<String, String>, RedisScanKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisConnection<String, String> connection;

        public RedisScanKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        @Override
        public RedisKeyDumpItemReader<String, String> build() {
            return new RedisKeyDumpItemReader<>(readTimeout, keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
        }
    }

    public static RedisLiveKeyDumpItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new RedisLiveKeyDumpItemReaderBuilder(pool, connection);
    }

    public static class RedisLiveKeyDumpItemReaderBuilder extends AbstractLiveKeyValueItemReaderBuilder<RedisKeyDumpItemReader<String, String>, RedisLiveKeyDumpItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisPubSubConnection<String, String> connection;

        public RedisLiveKeyDumpItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        @Override
        public RedisKeyDumpItemReader<String, String> build() {
            return new RedisKeyDumpItemReader<>(readTimeout, keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
        }
    }
}
