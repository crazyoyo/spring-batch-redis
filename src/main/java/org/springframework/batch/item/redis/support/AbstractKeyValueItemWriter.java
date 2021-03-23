package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class AbstractKeyValueItemWriter<K, V, C extends StatefulConnection<K, V>, T extends KeyValue<K, ?>> extends AbstractItemStreamItemWriter<T> {

    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<K, V>> commands;

    protected AbstractKeyValueItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands) {
        Assert.notNull(pool, "A connection pool is required");
        Assert.notNull(commands, "A command function is required");
        this.pool = pool;
        this.commands = commands;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            try {
                List<RedisFuture<?>> futures = write(items, commands);
                commands.flushCommands();
                long timeout = connection.getTimeout().toMillis();
                for (RedisFuture<?> future : futures) {
                    future.get(timeout, TimeUnit.MILLISECONDS);
                }
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract List<RedisFuture<?>> write(List<? extends T> items, BaseRedisAsyncCommands<K, V> commands);

}
