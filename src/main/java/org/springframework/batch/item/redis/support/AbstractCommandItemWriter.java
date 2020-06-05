package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.List;
import java.util.function.Function;

public abstract class AbstractCommandItemWriter<K, V, C extends StatefulConnection<K, V>, T> extends AbstractItemWriter<K, V, C, T> {

    protected AbstractCommandItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout) {
        super(pool, commands, commandTimeout);
    }

    @Override
    protected void write(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
        RedisFuture<?> future = write(commands, item);
        if (future == null) {
            return;
        }
        futures.add(future);
    }

    protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item);

}
