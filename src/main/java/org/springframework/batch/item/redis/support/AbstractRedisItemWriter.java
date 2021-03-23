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

public abstract class AbstractRedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

    private final GenericObjectPool<? extends StatefulConnection<K, V>> pool;
    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async;

    public AbstractRedisItemWriter(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async) {
        Assert.notNull(pool, "A connection pool is required");
        Assert.notNull(async, "An async command function is required");
        this.pool = pool;
        this.async = async;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
            commands.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = write(items, commands);
            commands.flushCommands();
            long commandTimeout = connection.getTimeout().toMillis();
            for (RedisFuture<?> future : futures) {
                future.get(commandTimeout, TimeUnit.MILLISECONDS);
            }
            commands.setAutoFlushCommands(true);
        }
    }

    protected abstract List<RedisFuture<?>> write(List<? extends T> items, BaseRedisAsyncCommands<K, V> commands);
}
