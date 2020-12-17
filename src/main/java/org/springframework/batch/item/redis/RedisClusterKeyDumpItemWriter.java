package org.springframework.batch.item.redis;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractDataStructureItemWriter;
import org.springframework.batch.item.redis.support.AbstractKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.RedisClusterConnectionPoolBuilder;
import org.springframework.batch.item.redis.support.RedisConnectionPoolBuilder;

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

    public static <K, V> RedisClusterKeyDumpItemWriterBuilder<K, V> builder() {
        return new RedisClusterKeyDumpItemWriterBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisClusterKeyDumpItemWriterBuilder<K, V> extends RedisClusterConnectionPoolBuilder<K, V, RedisClusterKeyDumpItemWriterBuilder<K, V>> {

        private boolean replace;

        public RedisClusterKeyDumpItemWriter<K, V> build() {
            return new RedisClusterKeyDumpItemWriter<>(getPool(), replace, commandTimeout);
        }

    }
}
