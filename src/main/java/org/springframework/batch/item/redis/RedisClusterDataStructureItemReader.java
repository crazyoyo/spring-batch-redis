package org.springframework.batch.item.redis;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemReader;
import org.springframework.batch.item.redis.support.JobOptions;
import org.springframework.batch.item.redis.support.RedisClusterKeyValueItemReaderBuilder;

import java.time.Duration;

public class RedisClusterDataStructureItemReader<K, V> extends AbstractDataStructureItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public RedisClusterDataStructureItemReader(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, ItemReader<K> keyReader, Duration commandTimeout, JobOptions jobOptions, int queueCapacity, Duration pollingTimeout) {
        super(keyReader, commandTimeout, jobOptions, queueCapacity, pollingTimeout);
        this.pool = pool;
    }

    @Override
    protected StatefulRedisClusterConnection<K, V> connection() throws Exception {
        return pool.borrowObject();
    }

    @Override
    protected BaseRedisAsyncCommands<K, V> commands(StatefulRedisClusterConnection<K, V> connection) {
        return connection.async();
    }

    public static <K, V> RedisClusterDataStructureItemReaderBuilder<K, V> builder() {
        return new RedisClusterDataStructureItemReaderBuilder<>();
    }

    public static class RedisClusterDataStructureItemReaderBuilder<K, V> extends RedisClusterKeyValueItemReaderBuilder<K, V, RedisClusterDataStructureItemReaderBuilder<K, V>> {

        public RedisClusterDataStructureItemReader<K, V> build() {
            return new RedisClusterDataStructureItemReader<>(getPool(), getKeyReader(), commandTimeout, jobOptions, queueCapacity, pollingTimeout);
        }

    }
}
