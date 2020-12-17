package org.springframework.batch.item.redis;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemWriter;
import org.springframework.batch.item.redis.support.RedisClusterConnectionPoolBuilder;

import java.time.Duration;

public class RedisClusterDataStructureItemWriter<K, V> extends AbstractDataStructureItemWriter<K, V, StatefulRedisClusterConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public RedisClusterDataStructureItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, Duration commandTimeout) {
        super(commandTimeout);
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

    public static <K, V> RedisClusterDataStructureItemWriterBuilder<K, V> builder() {
        return new RedisClusterDataStructureItemWriterBuilder<>();
    }

    public static class RedisClusterDataStructureItemWriterBuilder<K, V> extends RedisClusterConnectionPoolBuilder<K, V, RedisClusterDataStructureItemWriterBuilder<K, V>> {

        public RedisClusterDataStructureItemWriter<K, V> build() {
            return new RedisClusterDataStructureItemWriter<>(getPool(), commandTimeout);
        }

    }
}
