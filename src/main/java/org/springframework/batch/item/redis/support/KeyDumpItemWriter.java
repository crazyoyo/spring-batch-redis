package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class KeyDumpItemWriter<K, V, C extends StatefulConnection<K, V>> extends AbstractKeyValueItemWriter<K, V, C, KeyValue<K, byte[]>> {

    private final boolean replace;

    public KeyDumpItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands) {
        this(pool, commands, true);
    }

    public KeyDumpItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, boolean replace) {
        super(pool, commands);
        this.replace = replace;
    }




    @SuppressWarnings("unchecked")
    @Override
    protected List<RedisFuture<?>> write(List<? extends KeyValue<K, byte[]>> items, BaseRedisAsyncCommands<K, V> commands) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (KeyValue<K, byte[]> item : items) {
            futures.add(restore((RedisKeyAsyncCommands<K, V>) commands, item));
        }
        return futures;
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<?> restore(RedisKeyAsyncCommands<K, V> keyCommands, KeyValue<K, byte[]> item) {
        if (item.getValue() == null || item.noKeyTtl()) {
            return keyCommands.del(item.getKey());
        }
        RestoreArgs restoreArgs = new RestoreArgs().replace(replace);
        if (item.hasTtl()) {
            restoreArgs.ttl(item.getTtl() * 1000);
        }
        return keyCommands.restore(item.getKey(), item.getValue(), restoreArgs);
    }

}
