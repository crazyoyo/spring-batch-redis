package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionPoolBuilder;

import java.time.Duration;

public class RedisKeyDumpItemWriter<K, V> extends AbstractKeyDumpItemWriter<K, V, StatefulRedisConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public RedisKeyDumpItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, boolean replace, Duration commandTimeout) {
        super(replace, commandTimeout);
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

    public static <K, V> RedisKeyDumpItemWriterBuilder<K, V> builder() {
        return new RedisKeyDumpItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisKeyDumpItemWriterBuilder<K, V> extends RedisConnectionPoolBuilder<K, V, RedisKeyDumpItemWriterBuilder<K, V>> {

        private boolean replace;

        public RedisKeyDumpItemWriter<K, V> build() {
            return new RedisKeyDumpItemWriter<>(getPool(), replace, commandTimeout);
        }

    }
}
