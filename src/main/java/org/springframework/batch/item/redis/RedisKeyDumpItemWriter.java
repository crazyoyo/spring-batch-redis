package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.CommandTimeoutBuilder;

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

    public static RedisKeyDumpItemWriterBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool) {
        return new RedisKeyDumpItemWriterBuilder(pool);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisKeyDumpItemWriterBuilder extends CommandTimeoutBuilder<RedisKeyDumpItemWriterBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private boolean replace;

        public RedisKeyDumpItemWriterBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool) {
            this.pool = pool;
        }

        public RedisKeyDumpItemWriter<String, String> build() {
            return new RedisKeyDumpItemWriter<>(pool, replace, commandTimeout);
        }

    }
}
