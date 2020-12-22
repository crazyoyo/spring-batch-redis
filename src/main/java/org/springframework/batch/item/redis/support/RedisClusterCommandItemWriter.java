package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.time.Duration;
import java.util.function.BiFunction;

public class RedisClusterCommandItemWriter<K, V, T> extends AbstractCommandItemWriter<K, V, T, StatefulRedisClusterConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public RedisClusterCommandItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command, Duration commandTimeout) {
        super(command, commandTimeout);
        this.pool = pool;
    }

    @Override
    protected BaseRedisAsyncCommands<K, V> commands(StatefulRedisClusterConnection<K, V> connection) {
        return connection.async();
    }

    @Override
    protected StatefulRedisClusterConnection<K, V> connection() throws Exception {
        return pool.borrowObject();
    }

    public static <T> RedisClusterCommandItemWriterBuilder<T> builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, BiFunction<?, T, RedisFuture<?>> command) {
        return new RedisClusterCommandItemWriterBuilder<>(pool, (BiFunction) command);
    }

    public static class RedisClusterCommandItemWriterBuilder<T> extends CommandItemWriterBuilder<T, RedisClusterCommandItemWriterBuilder<T>> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;

        public RedisClusterCommandItemWriterBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, BiFunction<BaseRedisAsyncCommands<String, String>, T, RedisFuture<?>> command) {
            super(command);
            this.pool = pool;
        }

        public RedisClusterCommandItemWriter<String, String, T> build() {
            return new RedisClusterCommandItemWriter(pool, command, commandTimeout);
        }

    }

}
