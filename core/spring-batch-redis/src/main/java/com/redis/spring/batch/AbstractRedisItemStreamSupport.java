package com.redis.spring.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;

public abstract class AbstractRedisItemStreamSupport<K, V, I> extends ItemStreamSupport {

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    protected final AbstractRedisClient client;

    protected final RedisCodec<K, V> codec;

    private int poolSize = DEFAULT_POOL_SIZE;

    private GenericObjectPool<StatefulConnection<K, V>> pool;

    protected AbstractRedisItemStreamSupport(AbstractRedisClient client, RedisCodec<K, V> codec) {
        setName(ClassUtils.getShortName(getClass()));
        this.client = client;
        this.codec = codec;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        if (!isOpen()) {
            doOpen();
        }
        super.open(executionContext);
    }

    protected void doOpen() {
        GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolSize);
        pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(), config);
    }

    protected Supplier<StatefulConnection<K, V>> connectionSupplier() {
        return ConnectionUtils.supplier(client, codec);
    }

    public boolean isOpen() {
        return pool != null;
    }

    @Override
    public synchronized void close() {
        super.close();
        if (isOpen()) {
            doClose();
        }
    }

    protected void doClose() {
        pool.close();
        pool = null;
    }

    protected List<Object> execute(Collection<? extends I> items) throws Exception {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
            long timeout = connection.getTimeout().toMillis();
            BaseRedisAsyncCommands<K, V> commands = ConnectionUtils.async(connection);
            List<RedisFuture<?>> futures = new ArrayList<>();
            try {
                connection.setAutoFlushCommands(false);
                execute(commands, items, futures);
                connection.flushCommands();
                List<Object> results = new ArrayList<>(futures.size());
                for (RedisFuture<?> future : futures) {
                    results.add(future.get(timeout, TimeUnit.MILLISECONDS));
                }
                return results;
            } finally {
                connection.setAutoFlushCommands(true);
            }
        }
    }

    protected void execute(BaseRedisAsyncCommands<K, V> commands, Collection<? extends I> items, List<RedisFuture<?>> futures) {
        for (I item : items) {
            execute(commands, item, futures);
        }
    }

    protected abstract void execute(BaseRedisAsyncCommands<K, V> commands, I item, List<RedisFuture<?>> futures);

}
