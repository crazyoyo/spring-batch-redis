package org.springframework.batch.item.redis.support;

import com.redislabs.lettuce.helper.RedisOptions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

@Slf4j
public abstract class AbstractRedisItemReader<K, V, T> extends AbstractItemCountingItemStreamItemReader<T> {

    @Getter
    private final ItemReader<K> keyReader;
    private final GenericObjectPool<? extends StatefulConnection<K, V>> pool;
    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands;
    private final BlockingQueue<T> itemQueue;
    private final ExecutorService executor;
    private final List<BatchRunnable<K>> enqueuers;
    private final long commandTimeout;
    private final long queuePollingTimeout;

    protected AbstractRedisItemReader(ItemReader<K> keyReader, GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, int threadCount, int batchSize, Duration commandTimeout, int queueCapacity, long queuePollingTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(pool, "A connection pool is required.");
        Assert.notNull(commands, "A commands provider is required.");
        Assert.isTrue(threadCount > 0, "Thread count must be greater than 0.");
        Assert.isTrue(batchSize > 0, "Bach size must be greater than 0.");
        Assert.notNull(commandTimeout, "Command timeout is required.");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than 0.");
        Assert.isTrue(queuePollingTimeout > 0, "Queue polling timeout must be greater than 0.");
        this.keyReader = keyReader;
        this.pool = pool;
        this.commands = commands;
        this.commandTimeout = commandTimeout.getSeconds();
        this.itemQueue = new LinkedBlockingDeque<>(queueCapacity);
        this.queuePollingTimeout = queuePollingTimeout;
        this.executor = Executors.newFixedThreadPool(threadCount);
        this.enqueuers = new ArrayList<>(threadCount);
        for (int index = 0; index < threadCount; index++) {
            enqueuers.add(new BatchRunnable<K>(keyReader, this::write, batchSize));
        }
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
            item = itemQueue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
        } while (item == null && !executor.isTerminated());
        return item;
    }

    protected <F> F get(RedisFuture<F> future) throws InterruptedException, ExecutionException, TimeoutException {
        if (future == null) {
            return null;
        }
        return future.get(commandTimeout, TimeUnit.SECONDS);
    }

    protected long getTtl(RedisFuture<Long> future) throws InterruptedException, ExecutionException, TimeoutException {
        Long ttl = get(future);
        if (ttl == null) {
            return 0;
        }
        return ttl;
    }

    public List<T> read(List<? extends K> items) throws Exception {
        try (StatefulConnection<K, V> connection = pool.borrowObject()) {
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

    public static class AbstractRedisItemReaderBuilder {

        private static final String KEYSPACE_CHANNEL_PREFIX = "__keyspace@";
        private static final String KEYSPACE_CHANNEL_SUFFIX = "__:";

        protected ItemReader<String> keyReader(RedisOptions redisOptions, ReaderOptions readerOptions) {

            if (readerOptions.isLive()) {
                return new LiveKeyItemReader<>(redisOptions.connection(), redisOptions.sync(), scanArgs(readerOptions), redisOptions.pubSubConnection(), readerOptions.getKeyspaceNotificationQueueOptions().getCapacity(), readerOptions.getKeyspaceNotificationQueueOptions().getPollingTimeout(), pubSubPattern(redisOptions, readerOptions), new StringChannelConverter());
            }
            return new KeyItemReader<>(redisOptions.connection(), redisOptions.sync(), scanArgs(readerOptions));
        }

        private ScanArgs scanArgs(ReaderOptions readerOptions) {
            return ScanArgs.Builder.limit(readerOptions.getScanCount()).match(readerOptions.getScanMatch());
        }

        private String pubSubPattern(RedisOptions redisOptions, ReaderOptions readerOptions) {
            Assert.notNull(readerOptions.getScanMatch(), "A scan match pattern is required.");
            return KEYSPACE_CHANNEL_PREFIX + redisOptions.getRedisURI().getDatabase() + KEYSPACE_CHANNEL_SUFFIX + readerOptions.getScanMatch();
        }

    }

}
