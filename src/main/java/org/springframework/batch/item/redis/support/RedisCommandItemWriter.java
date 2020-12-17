package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.function.BiFunction;

public class RedisCommandItemWriter<K, V, T> extends AbstractCommandItemWriter<K, V, T, StatefulRedisConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    public RedisCommandItemWriter(GenericObjectPool<StatefulRedisConnection<K, V>> pool, BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command, Duration commandTimeout) {
        super(command, commandTimeout);
        this.pool = pool;
    }

    @Override
    protected BaseRedisAsyncCommands<K, V> commands(StatefulRedisConnection<K, V> connection) {
        return connection.async();
    }

    @Override
    protected StatefulRedisConnection<K, V> connection() throws Exception {
        return pool.borrowObject();
    }

    public static <K, V, T> RedisCommandItemWriterBuilder<K, V, T> builder() {
        return new RedisCommandItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisCommandItemWriterBuilder<K, V, T> extends AbstractCommandItemWriterBuilder<K, V, T, RedisCommandItemWriterBuilder<K, V, T>> {

        private GenericObjectPool<StatefulRedisConnection<K, V>> pool;

        public RedisCommandItemWriter<K, V, T> build() {
            Assert.notNull(pool, "A connection pool is required.");
            return new RedisCommandItemWriter<>(pool, getCommand(), timeout);
        }

    }
}
