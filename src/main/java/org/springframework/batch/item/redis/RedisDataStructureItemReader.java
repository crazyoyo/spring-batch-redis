package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.DataStructureItemReader;
import org.springframework.batch.item.redis.support.AbstractLiveKeyValueItemReaderBuilder;
import org.springframework.batch.item.redis.support.AbstractScanKeyValueItemReaderBuilder;

import java.time.Duration;
import java.util.function.Function;

public class RedisDataStructureItemReader<K, V> extends DataStructureItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisDataStructureItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity) {
        super(readTimeout, keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity);
    }

    public RedisDataStructureItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(readTimeout, keyReader, pool, StatefulRedisConnection::async, chunkSize, threads, queueCapacity, stepBuilderProvider);
    }

    public static RedisScanDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new RedisScanDataStructureItemReaderBuilder(pool, connection);
    }

    public static class RedisScanDataStructureItemReaderBuilder extends AbstractScanKeyValueItemReaderBuilder<RedisDataStructureItemReader<String, String>, RedisScanDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisConnection<String, String> connection;

        public RedisScanDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        @Override
        public RedisDataStructureItemReader<String, String> build() {
            return new RedisDataStructureItemReader<>(readTimeout, keyReader(connection), pool, chunkSize, threadCount, queueCapacity);
        }
    }

    public static RedisLiveDataStructureItemReaderBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
        return new RedisLiveDataStructureItemReaderBuilder(pool, connection);
    }

    public static class RedisLiveDataStructureItemReaderBuilder extends AbstractLiveKeyValueItemReaderBuilder<RedisDataStructureItemReader<String, String>, RedisLiveDataStructureItemReaderBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private final StatefulRedisPubSubConnection<String, String> connection;

        public RedisLiveDataStructureItemReaderBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> connection) {
            this.pool = pool;
            this.connection = connection;
        }

        @Override
        public RedisDataStructureItemReader<String, String> build() {
            return new RedisDataStructureItemReader<>(readTimeout, keyspaceNotificationReader(connection), pool, chunkSize, threadCount, queueCapacity, stepBuilderProvider());
        }
    }

}
