package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public abstract class AbstractItemWriter<K, V, C extends StatefulConnection<K, V>, T> extends AbstractItemStreamItemWriter<T> {

    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<K, V>> async;

    protected AbstractItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> async) {
        Assert.notNull(pool, "A connection pool is required");
        Assert.notNull(async, "An async command function is required");
        this.pool = pool;
        this.async = async;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
            commands.setAutoFlushCommands(false);
            try {
                write(commands, connection.getTimeout(), items);
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract void write(BaseRedisAsyncCommands<K,V> commands, Duration timeout, List<? extends T> items) throws InterruptedException, ExecutionException, TimeoutException;

}
