package org.springframework.batch.item.redis.support;

import com.redislabs.lettuce.helper.RedisOptions;
import io.lettuce.core.ScanArgs;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> {

    @Getter
    private final ItemReader<K> keyReader;
    private final ItemProcessor<List<? extends K>, List<T>> valueProcessor;
    private final BlockingQueue<T> itemQueue;
    private final ExecutorService executor;
    private final List<BatchRunnable<K>> enqueuers;
    private final long queuePollingTimeout;

    public RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueProcessor, ReaderOptions options) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(valueProcessor, "A key/value processor is required.");
        Assert.notNull(options, "Reader options are required.");
        this.keyReader = keyReader;
        this.valueProcessor = valueProcessor;
        this.itemQueue = new LinkedBlockingDeque<>(options.getItemQueueOptions().getCapacity());
        this.queuePollingTimeout = options.getItemQueueOptions().getPollingTimeout();
        this.executor = Executors.newFixedThreadPool(options.getThreadCount());
        this.enqueuers = new ArrayList<>(options.getThreadCount());
        for (int index = 0; index < options.getThreadCount(); index++) {
            enqueuers.add(new BatchRunnable<K>(keyReader, this::write, options.getBatchSize()));
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
        List<T> values = valueProcessor.process(keys);
        if (values == null) {
            return;
        }
        itemQueue.addAll(values);
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

    public static RedisItemReaderBuilder builder() {
        return new RedisItemReaderBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisItemReaderBuilder {

        private static final String KEYSPACE_CHANNEL_PREFIX = "__keyspace@";
        private static final String KEYSPACE_CHANNEL_SUFFIX = "__:";

        protected ItemReader<String> keyReader(RedisOptions redisOptions, ReaderOptions options) {
            Assert.notNull(redisOptions, "Redis options are required.");
            Assert.notNull(options, "Reader options are required.");
            if (options.isLive()) {
                return new LiveKeyItemReader<>(redisOptions.connection(), redisOptions.sync(), scanArgs(options), redisOptions.pubSubConnection(), options.getItemQueueOptions(), pubSubPattern(redisOptions, options), new StringChannelConverter());
            }
            return new KeyItemReader<>(redisOptions.connection(), redisOptions.sync(), scanArgs(options));
        }

        private ScanArgs scanArgs(ReaderOptions options) {
            return ScanArgs.Builder.limit(options.getScanCount()).match(options.getScanMatch());
        }

        private String pubSubPattern(RedisOptions redisOptions, ReaderOptions readerOptions) {
            Assert.notNull(readerOptions.getScanMatch(), "A scan match pattern is required.");
            return KEYSPACE_CHANNEL_PREFIX + redisOptions.getRedisURI().getDatabase() + KEYSPACE_CHANNEL_SUFFIX + readerOptions.getScanMatch();
        }

    }

}
