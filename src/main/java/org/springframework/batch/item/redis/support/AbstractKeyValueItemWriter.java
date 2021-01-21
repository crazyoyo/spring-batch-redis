package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class AbstractKeyValueItemWriter<K, V, T extends KeyValue<K, ?>> extends AbstractItemStreamItemWriter<T> {

    protected final long commandTimeout;
    private final GenericObjectPool<? extends StatefulConnection<K, V>> pool;
    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands;

    protected AbstractKeyValueItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout) {
        Assert.notNull(pool, "A connection pool is required");
        Assert.notNull(commands, "A command function is required");
        Assert.notNull(commandTimeout, "Command timeout is required");
        this.pool = pool;
        this.commands = commands;
        this.commandTimeout = commandTimeout.getSeconds();
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            try {
                List<RedisFuture<?>> futures = write(items, commands);
                commands.flushCommands();
                for (RedisFuture<?> future : futures) {
                    future.get(commandTimeout, TimeUnit.SECONDS);
                }
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract List<RedisFuture<?>> write(List<? extends T> items, BaseRedisAsyncCommands<K, V> commands);

}
