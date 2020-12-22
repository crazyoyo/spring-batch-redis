package org.springframework.batch.item.redis;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemWriter;
import org.springframework.batch.item.redis.support.CommandTimeoutBuilder;

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

    public static RedisClusterDataStructureItemWriterBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool) {
        return new RedisClusterDataStructureItemWriterBuilder(pool);
    }

    public static class RedisClusterDataStructureItemWriterBuilder extends CommandTimeoutBuilder<RedisClusterDataStructureItemWriterBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;

        public RedisClusterDataStructureItemWriterBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool) {
            this.pool = pool;
        }

        public RedisClusterDataStructureItemWriter<String, String> build() {
            return new RedisClusterDataStructureItemWriter<>(pool, commandTimeout);
        }

    }
}
