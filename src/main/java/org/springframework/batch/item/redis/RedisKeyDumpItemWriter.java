package org.springframework.batch.item.redis;

import com.redislabs.lettuce.helper.RedisOptions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
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

        private RedisOptions redisOptions;
        private boolean replace;

        public RedisKeyDumpItemWriter<String, String> build() {
            Assert.notNull(redisOptions, "Redis options are required.");
            return new RedisKeyDumpItemWriter<>(redisOptions.connectionPool(), redisOptions.async(), redisOptions.getTimeout(), replace);
        }

    }
}
