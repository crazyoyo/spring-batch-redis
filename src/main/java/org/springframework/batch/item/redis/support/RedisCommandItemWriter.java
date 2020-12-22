package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;

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

    public static <T> RedisCommandItemWriterBuilder<T> builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, BiFunction<?, T, RedisFuture<?>> command) {
        return new RedisCommandItemWriterBuilder<>(pool, (BiFunction) command);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisCommandItemWriterBuilder<T> extends CommandItemWriterBuilder<T, RedisCommandItemWriterBuilder<T>> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;

        public RedisCommandItemWriterBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool, BiFunction<BaseRedisAsyncCommands<String, String>, T, RedisFuture<?>> command) {
            super(command);
            this.pool = pool;
        }

        public RedisCommandItemWriter<String, String, T> build() {
            return new RedisCommandItemWriter<>(pool, command, commandTimeout);
        }

    }
}
