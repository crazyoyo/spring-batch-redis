package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemWriter;
import org.springframework.batch.item.redis.support.CommandTimeoutBuilder;

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

    public static RedisDataStructureItemWriterBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool) {
        return new RedisDataStructureItemWriterBuilder(pool);
    }

    public static class RedisDataStructureItemWriterBuilder extends CommandTimeoutBuilder<RedisDataStructureItemWriterBuilder> {

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;

        public RedisDataStructureItemWriterBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool) {
            this.pool = pool;
        }

        public RedisDataStructureItemWriter<String, String> build() {
            return new RedisDataStructureItemWriter<>(pool, commandTimeout);
        }

    }
}
