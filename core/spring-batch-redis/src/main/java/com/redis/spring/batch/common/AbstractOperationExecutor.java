package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;

import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public abstract class AbstractOperationExecutor<K, V, I, O> extends ItemStreamSupport implements OperationExecutor<I, O> {

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private ReadFrom readFrom;

    private int poolSize = DEFAULT_POOL_SIZE;

    private GenericObjectPool<StatefulConnection<K, V>> pool;

    private BatchOperation<K, V, I, O> operation;

    protected AbstractOperationExecutor(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.client = client;
        this.codec = codec;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (operation == null) {
            Supplier<StatefulConnection<K, V>> connectionSupplier = ConnectionUtils.supplier(client, codec, readFrom);
            GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
            config.setMaxTotal(poolSize);
            pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, config);
            operation = operation();
        }
    }

    protected abstract BatchOperation<K, V, I, O> operation();

    public boolean isOpen() {
        return operation != null;
    }

    @Override
    public synchronized void close() {
        if (operation != null) {
            pool.close();
            operation = null;
        }
        super.close();
    }

    @Override
    public List<O> execute(List<? extends I> items) {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
            long timeout = connection.getTimeout().toMillis();
            BaseRedisAsyncCommands<K, V> commands = ConnectionUtils.async(connection);
            try {
                connection.setAutoFlushCommands(false);
                List<RedisFuture<O>> futures = operation.execute(commands, items);
                connection.flushCommands();
                List<O> results = new ArrayList<>(futures.size());
                for (RedisFuture<? extends O> future : futures) {
                    results.add(future.get(timeout, TimeUnit.MILLISECONDS));
                }
                return results;
            } finally {
                connection.setAutoFlushCommands(true);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ItemStreamException("Command execution interrupted", e);
        } catch (ExecutionException e) {
            throw new ItemStreamException("Exception during command execution", e);
        } catch (TimeoutException e) {
            throw new ItemStreamException("Timed out waiting for command response", e);
        } catch (Exception e) {
            throw new ItemStreamException("Could not get connection from pool", e);
        }
    }

}
