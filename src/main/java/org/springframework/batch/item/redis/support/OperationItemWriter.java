package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class OperationItemWriter<K, V, C extends StatefulConnection<K, V>, T> extends AbstractItemWriter<K, V, C, T> {

    private final RedisOperation<K, V, T> operation;

    public OperationItemWriter(GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(pool, async);
        Assert.notNull(operation, "A Redis operation is required");
        this.operation = operation;
    }

    @Override
    protected void write(BaseRedisAsyncCommands<K, V> commands, Duration timeout, List<? extends T> items) throws InterruptedException, ExecutionException, TimeoutException {
        List<RedisFuture<?>> futures = write(commands, items);
        commands.flushCommands();
        long timeoutMillis = timeout.toMillis();
        for (RedisFuture<?> future : futures) {
            if (future != null) {
                future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    protected List<RedisFuture<?>> write(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            futures.add(operation.execute(commands, item));
        }
        return futures;
    }


}
