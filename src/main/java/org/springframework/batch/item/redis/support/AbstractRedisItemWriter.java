package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public abstract class AbstractRedisItemWriter<K, V, T, C extends StatefulConnection<K, V>> extends AbstractItemStreamItemWriter<T> {

    private final long commandTimeout;

    public AbstractRedisItemWriter(Duration commandTimeout) {
        this.commandTimeout = commandTimeout.getSeconds();
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (C connection = connection()) {
            BaseRedisAsyncCommands<K, V> commands = commands(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = write(items, commands);
            commands.flushCommands();
            for (RedisFuture<?> future : futures) {
                future.get(commandTimeout, TimeUnit.SECONDS);
            }
            commands.setAutoFlushCommands(true);
        }
    }

    protected abstract C connection() throws Exception;

    protected abstract BaseRedisAsyncCommands<K,V> commands(C connection);

    protected abstract List<RedisFuture<?>> write(List<? extends T> items, BaseRedisAsyncCommands<K, V> commands);
}
