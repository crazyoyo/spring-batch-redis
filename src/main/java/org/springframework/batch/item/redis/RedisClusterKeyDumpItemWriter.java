package org.springframework.batch.item.redis;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.CommandTimeoutBuilder;

import java.time.Duration;

public class RedisClusterKeyDumpItemWriter<K, V> extends AbstractKeyDumpItemWriter<K, V, StatefulRedisClusterConnection<K, V>> {

    private final GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;

    public RedisClusterKeyDumpItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, boolean replace, Duration commandTimeout) {
        super(replace, commandTimeout);
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

    public static RedisClusterKeyDumpItemWriterBuilder builder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool) {
        return new RedisClusterKeyDumpItemWriterBuilder(pool);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisClusterKeyDumpItemWriterBuilder extends CommandTimeoutBuilder<RedisClusterKeyDumpItemWriterBuilder> {

        private final GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool;
        private boolean replace;

        public RedisClusterKeyDumpItemWriterBuilder(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool) {
            this.pool = pool;
        }

        public RedisClusterKeyDumpItemWriter<String, String> build() {
            return new RedisClusterKeyDumpItemWriter<>(pool, replace, commandTimeout);
        }

    }
}
