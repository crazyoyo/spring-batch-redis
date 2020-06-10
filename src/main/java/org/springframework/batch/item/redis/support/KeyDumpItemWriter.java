package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.KeyDump;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class KeyDumpItemWriter<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemWriter<K, V, C, KeyDump<K>> {

    private final boolean replace;

    public KeyDumpItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout, boolean replace) {
        super(pool, commands, commandTimeout);
        this.replace = replace;
    }

    @Override
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyDump<K> item) {
        futures.add(((RedisKeyAsyncCommands<K, V>) commands).restore(item.getKey(), item.getValue(), new RestoreArgs().replace(replace)));
    }

    @Override
    protected void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, KeyDump<K> item, long ttl) {
        futures.add(((RedisKeyAsyncCommands<K, V>) commands).restore(item.getKey(), item.getValue(), new RestoreArgs().ttl(ttl).replace(replace)));
    }
}
