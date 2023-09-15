package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemStreamException;

import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class BatchOperationFunction<K, V, I, O> implements Function<List<? extends I>, List<O>> {

    private final GenericObjectPool<StatefulConnection<K, V>> pool;

    private final BatchOperation<K, V, I, O> operation;

    public BatchOperationFunction(GenericObjectPool<StatefulConnection<K, V>> pool, BatchOperation<K, V, I, O> operation) {
        this.pool = pool;
        this.operation = operation;
    }

    @Override
    public List<O> apply(List<? extends I> items) {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
            return apply(connection, items);
        } catch (ItemStreamException e) {
            throw e;
        } catch (Exception e) {
            throw new ItemStreamException("Could not get connection from pool", e);
        }
    }

    private List<O> apply(StatefulConnection<K, V> connection, List<? extends I> items) {
        long timeout = connection.getTimeout().toMillis();
        BaseRedisAsyncCommands<K, V> commands = ConnectionUtils.async(connection);
        try {
            connection.setAutoFlushCommands(false);
            List<RedisFuture<O>> futures = operation.execute(commands, items);
            connection.flushCommands();
            List<O> results = new ArrayList<>(futures.size());
            for (RedisFuture<? extends O> future : futures) {
                try {
                    results.add(future.get(timeout, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ItemStreamException("Interrupted", e);
                } catch (ExecutionException e) {
                    throw new ItemStreamException("Execution exception: " + future.getError(), e);
                } catch (TimeoutException e) {
                    throw new ItemStreamException("Command timeout", e);
                }
            }
            return results;
        } finally {
            connection.setAutoFlushCommands(true);
        }
    }

    public interface BatchOperation<K, V, I, O> {

        List<RedisFuture<O>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items);

    }

}
