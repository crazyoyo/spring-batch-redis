package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractCommandItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

    protected AbstractCommandItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout) {
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
