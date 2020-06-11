package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
import org.springframework.batch.item.redis.support.PoolOptions;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public class RedisKeyDumpItemWriter<K, V> extends AbstractKeyValueItemWriter<K, V, KeyDump<K>> {

    private final boolean replace;

    public RedisKeyDumpItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, boolean replace) {
        super(pool, commands, commandTimeout);
        this.replace = replace;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyDump<K> item) {
        futures.add(((RedisKeyAsyncCommands<K, V>) commands).restore(item.getKey(), item.getValue(), new RestoreArgs().replace(replace)));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyDump<K> item, long ttl) {
        futures.add(((RedisKeyAsyncCommands<K, V>) commands).restore(item.getKey(), item.getValue(), new RestoreArgs().ttl(ttl).replace(replace)));
    }

    public static RedisKeyDumpItemWriterBuilder builder() {
        return new RedisKeyDumpItemWriterBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisKeyDumpItemWriterBuilder {

        private RedisURI redisURI;
        private boolean cluster;
        private PoolOptions poolOptions = PoolOptions.builder().build();
        private boolean replace;

        public RedisKeyDumpItemWriter<String, String> build() {
            Assert.notNull(redisURI, "A Redis URI is required.");
            Assert.notNull(poolOptions, "Pool options are required.");
            return new RedisKeyDumpItemWriter<>(poolOptions.create(redisURI, cluster), async(), redisURI.getTimeout(), replace);
        }

        protected Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async() {
            if (cluster) {
                return c -> ((StatefulRedisClusterConnection<String, String>) c).async();
            }
            return c -> ((StatefulRedisConnection<String, String>) c).async();
        }

    }
}
