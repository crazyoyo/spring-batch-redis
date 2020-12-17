package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionPoolBuilder;

import java.time.Duration;

public class RedisDataStructureItemWriter<K, V> extends AbstractDataStructureItemWriter<K, V, StatefulRedisConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public RedisDataStructureItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, Duration commandTimeout) {
        super(commandTimeout);
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

    public static <K, V> RedisDataStructureItemWriterBuilder<K, V> builder() {
        return new RedisDataStructureItemWriterBuilder<>();
    }

    public static class RedisDataStructureItemWriterBuilder<K, V> extends RedisConnectionPoolBuilder<K, V, RedisDataStructureItemWriterBuilder<K, V>> {

        public RedisDataStructureItemWriter<K, V> build() {
            return new RedisDataStructureItemWriter<>(getPool(), commandTimeout);
        }

    }
}
