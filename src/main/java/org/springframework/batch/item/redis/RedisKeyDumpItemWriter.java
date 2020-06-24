package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractKeyValueItemWriter;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

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

    public static class RedisKeyDumpItemWriterBuilder extends RedisConnectionBuilder<RedisKeyDumpItemWriterBuilder> {

        private boolean replace;

        public RedisKeyDumpItemWriterBuilder replace(boolean replace) {
            this.replace = replace;
            return this;
        }

        public RedisKeyDumpItemWriter<String, String> build() {
            return new RedisKeyDumpItemWriter<>(pool(), async(), timeout(), replace);
        }

    }
}
