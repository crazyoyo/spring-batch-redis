package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractKeyDumpItemReader;
import org.springframework.batch.item.redis.support.JobOptions;
import org.springframework.batch.item.redis.support.RedisKeyValueItemReaderBuilder;

import java.time.Duration;

public class RedisKeyDumpItemReader<K, V> extends AbstractKeyDumpItemReader<K, V, StatefulRedisConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public RedisKeyDumpItemReader(GenericObjectPool<StatefulRedisConnection<K, V>> pool, ItemReader<K> keyReader, Duration commandTimeout, JobOptions jobOptions, int queueCapacity, Duration pollingTimeout) {
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


    public static <K, V> RedisKeyDumpItemReaderBuilder<K, V> builder() {
        return new RedisKeyDumpItemReaderBuilder<>();
    }

    public static class RedisKeyDumpItemReaderBuilder<K, V> extends RedisKeyValueItemReaderBuilder<K, V, RedisKeyDumpItemReaderBuilder<K, V>> {

        public RedisKeyDumpItemReader<K, V> build() {
            return new RedisKeyDumpItemReader<>(getPool(), getKeyReader(), commandTimeout, jobOptions, queueCapacity, pollingTimeout);
        }

    }
}
