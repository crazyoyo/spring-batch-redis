package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemReader;
import org.springframework.batch.item.redis.support.JobOptions;
import org.springframework.batch.item.redis.support.RedisKeyValueItemReaderBuilder;

import java.time.Duration;

public class RedisDataStructureItemReader<K, V> extends AbstractDataStructureItemReader<K, V, StatefulRedisConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public RedisDataStructureItemReader(GenericObjectPool<StatefulRedisConnection<K, V>> pool, ItemReader<K> keyReader, Duration commandTimeout, JobOptions jobOptions, int queueCapacity, Duration pollingTimeout) {
        super(keyReader, commandTimeout, jobOptions, queueCapacity, pollingTimeout);
        this.pool = pool;
    }

    @Override
    protected StatefulRedisConnection<K, V> connection() throws Exception {
        return pool.borrowObject();
    }

    @Override
    protected BaseRedisAsyncCommands<K, V> commands(StatefulRedisConnection<K, V> connection) {
        return connection.async();
    }

    public static <K, V> RedisDataStructureItemReaderBuilder<K, V> builder() {
        return new RedisDataStructureItemReaderBuilder<>();
    }

    public static class RedisDataStructureItemReaderBuilder<K, V> extends RedisKeyValueItemReaderBuilder<K, V, RedisDataStructureItemReaderBuilder<K, V>> {

        public RedisDataStructureItemReader<K, V> build() {
            return new RedisDataStructureItemReader<>(getPool(), getKeyReader(), commandTimeout, jobOptions, queueCapacity, pollingTimeout);
        }

    }
}
