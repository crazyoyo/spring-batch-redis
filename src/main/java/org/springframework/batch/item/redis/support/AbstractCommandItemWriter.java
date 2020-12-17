package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public abstract class AbstractCommandItemWriter<K, V, T, C extends StatefulConnection<K, V>> extends AbstractRedisItemWriter<K, V, T, C> {

    private final BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command;

    public AbstractCommandItemWriter(BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command, Duration commandTimeout) {
        super(commandTimeout);
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
