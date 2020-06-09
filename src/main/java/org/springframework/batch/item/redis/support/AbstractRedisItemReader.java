package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

@Slf4j
public abstract class AbstractRedisItemReader<K, V, C extends StatefulConnection<K, V>, T> extends AbstractItemCountingItemStreamItemReader<T> {

    @Getter
    private final ItemReader<K> keyReader;
    private final GenericObjectPool<C> pool;
    private final Function<C, BaseRedisAsyncCommands<K, V>> commands;
    private final ReaderOptions options;
    private final BlockingQueue<T> itemQueue;
    private final ExecutorService executor;
    private final List<BatchRunnable<K>> enqueuers;

    protected AbstractRedisItemReader(ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, ReaderOptions options) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(pool, "A connection pool is required.");
        Assert.notNull(commands, "A commands provider is required.");
        Assert.notNull(options, "Options are required.");
        this.keyReader = keyReader;
        this.pool = pool;
        this.commands = commands;
        this.options = options;
        this.itemQueue = new LinkedBlockingDeque<>(options.getQueueCapacity());
        this.executor = Executors.newFixedThreadPool(options.getThreadCount());
        this.enqueuers = new ArrayList<>(options.getThreadCount());
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).close();
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).update(executionContext);
        }
    }

    @Override
    protected void doOpen() {
        for (int index = 0; index < options.getThreadCount(); index++) {
            enqueuers.add(new BatchRunnable<K>(keyReader, this::write, options.getBatchSize()));
        }
        enqueuers.forEach(executor::submit);
        executor.shutdown();
    }

    private void write(List<? extends K> keys) throws Exception {
        itemQueue.addAll(read(keys));
    }

    @Override
    protected void doClose() throws ItemStreamException {
        if (executor.isTerminated()) {
            return;
        }
        executor.shutdownNow();
    }

    public void flush() {
        for (BatchRunnable<K> enqueuer : enqueuers) {
            try {
                enqueuer.flush();
            } catch (Exception e) {
                log.error("Could not flush", e);
            }
        }
    }

    @Override
    protected T doRead() throws Exception {
        T item;
        do {
            item = itemQueue.poll(options.getQueuePollingTimeout(), TimeUnit.MILLISECONDS);
        } while (item == null && !executor.isTerminated());
        return item;
    }

    protected <F> F get(RedisFuture<F> future) throws InterruptedException, ExecutionException, TimeoutException {
        if (future == null) {
            return null;
        }
        return future.get(options.getCommandTimeout(), TimeUnit.SECONDS);
    }

    protected long getTtl(RedisFuture<Long> future) throws InterruptedException, ExecutionException, TimeoutException {
        Long ttl = get(future);
        if (ttl == null) {
            return 0;
        }
        return ttl;
    }

    public List<T> read(List<? extends K> items) throws Exception {
        try (C connection = pool.borrowObject()) {
            BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
            commands.setAutoFlushCommands(false);
            try {
                return values(items, commands);
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract List<T> values(List<? extends K> items, BaseRedisAsyncCommands<K, V> commands) throws Exception;

}
