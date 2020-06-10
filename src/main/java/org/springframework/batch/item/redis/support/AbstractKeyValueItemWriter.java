package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.List;
import java.util.function.Function;

public abstract class AbstractKeyValueItemWriter<K, V, C extends StatefulConnection<K, V>, T extends AbstractKeyValue<K, ?>> extends AbstractRedisItemWriter<K, V, C, T> {

    protected AbstractKeyValueItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout) {
        super(pool, commands, commandTimeout);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void write(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
        if (item.getValue() == null || item.getTtl() == AbstractKeyValue.TTL_NOT_EXISTS) {
            futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey()));
        } else {
            if (item.getTtl() >= 0) {
                doWrite(commands, futures, item, item.getTtl());
            } else {
                doWrite(commands, futures, item);
            }
        }
    }

    protected abstract void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item);

    protected abstract void doWrite(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, long ttl);

}
