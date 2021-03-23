package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class CommandItemWriter<K, V, C extends StatefulConnection<K, V>, T> extends AbstractRedisItemWriter<K, V, C, T> {

    private final BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command;

    public CommandItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> async, BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command) {
        super(pool, async);
        this.command = command;
    }

    @Override
    protected List<RedisFuture<?>> write(List<? extends T> items, BaseRedisAsyncCommands<K, V> commands) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            RedisFuture<?> future = command.apply(commands, item);
            if (future != null) {
                futures.add(future);
            }
        }
        return futures;
    }

}
