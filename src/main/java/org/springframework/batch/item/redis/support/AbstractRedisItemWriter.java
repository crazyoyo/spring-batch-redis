package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
public abstract class AbstractRedisItemWriter<K, V, C extends StatefulConnection<K, V>, T> extends AbstractItemStreamItemWriter<T> {


    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<K, V>> commands;
    private final long commandTimeout;

    public AbstractRedisItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, long commandTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(pool, "A connection pool is required.");
        Assert.notNull(commands, "A commands provider is required.");
        this.pool = pool;
        this.commands = commands;
        this.commandTimeout = commandTimeout;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>();
            for (T item : items) {
                write(commands, futures, item);
            }
            commands.flushCommands();
            for (RedisFuture<?> future : futures) {
                try {
                    future.get(commandTimeout, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    log.error("Could not write item", e.getCause());
                } catch (TimeoutException e) {
                    log.error("Command timed out", e);
                }
            }
            commands.setAutoFlushCommands(true);
        }
    }


    protected abstract void write(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item);


}
