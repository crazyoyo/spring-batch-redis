package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> {

    @Data
    @Builder
    public static class Options {

        public static final int DEFAULT_BATCH_SIZE = 50;
        public static final int DEFAULT_THREADS = 1;
        public static final int DEFAULT_QUEUE_CAPACITY = 10000;
        public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;

        @Builder.Default
        private final int batchSize = DEFAULT_BATCH_SIZE;
        @Builder.Default
        private final int threads = DEFAULT_THREADS;
        @Builder.Default
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        @Builder.Default
        private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

    }

    private final Options options;
    @Getter
    private final ItemReader<K> keyReader;
    private final ItemProcessor<List<? extends K>, List<? extends T>> valueReader;

    private BlockingQueue<T> valueQueue;
    private ExecutorService executor;
    private List<Batcher<K>> threads;

    protected RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<? extends T>> valueReader, Options options) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(valueReader, "A value reader is required.");
        Assert.notNull(options, "Options are required.");
        this.keyReader = keyReader;
        this.valueReader = valueReader;
        this.options = options;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).open(executionContext);
        }
        super.open(executionContext);
    }

    @Override
    protected void doOpen() {
        valueQueue = new LinkedBlockingDeque<>(options.getQueueCapacity());
        threads = new ArrayList<>(options.getThreads());
        executor = Executors.newFixedThreadPool(options.getThreads());
        for (int index = 0; index < options.getThreads(); index++) {
            Batcher<K> thread = Batcher.<K>builder().batchSize(options.getBatchSize()).reader(keyReader).writer(ProcessingItemWriter.<K, T>builder().processor(valueReader).writer(QueueItemWriter.<T>builder().queue(valueQueue).build()).build()).build();
            threads.add(thread);
            executor.submit(thread);
        }
        executor.shutdown();
    }

    public void flush() {
        for (Batcher<K> thread : threads) {
            try {
                thread.flush();
            } catch (Exception e) {
                log.error("Could not flush batcher", e);
            }
        }
    }

    @Override
    public void close() throws ItemStreamException {
        super.close();
        if (keyReader instanceof ItemStream) {
            ((ItemStream) keyReader).close();
        }
    }

    @Override
    protected void doClose() {
        for (Batcher<K> batcher : threads) {
            batcher.stop();
        }
        try {
            executor.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interruped while waiting for executor termination");
        }
        threads = null;
        executor = null;
        if (!valueQueue.isEmpty()) {
            log.warn("Closing reader with non-empty value queue");
        }
        valueQueue = null;
    }

    @Override
    protected T doRead() throws Exception {
        T value;
        do {
            value = valueQueue.poll(options.getQueuePollingTimeout(), TimeUnit.MILLISECONDS);
        } while (value == null && !executor.isTerminated());
        return value;
    }

    protected static abstract class AbstractRedisItemReaderBuilder {

        public enum Mode {
            SCAN, NEW, LIVE
        }

        public static final RedisOptions DEFAULT_REDIS_OPTIONS = RedisOptions.builder().build();
        public static final Options DEFAULT_OPTIONS = Options.builder().build();
        public static final Mode DEFAULT_MODE = Mode.SCAN;
        public static final ScanArgs DEFAULT_SCAN_OPTIONS = new ScanArgs();
        private static final String DATABASE_TOKEN = "database";
        private static final String KEYSPACE_CHANNEL_TEMPLATE = "__keyspace@${" + DATABASE_TOKEN + "}__:*";

        private String[] patterns(RedisOptions redisOptions) {
            Map<String, String> variables = new HashMap<>();
            variables.put(DATABASE_TOKEN, String.valueOf(redisOptions.getRedisURI().getDatabase()));
            StringSubstitutor substitutor = new StringSubstitutor(variables);
            String pattern = substitutor.replace(KEYSPACE_CHANNEL_TEMPLATE);
            return new String[]{pattern};
        }

        private ItemReader<String> multiplexingReader(Options options, ItemReader<String> notificationReader, ItemReader<String> scanReader) {
            return MultiplexingItemReader.<String>builder().queueCapacity(options.getQueueCapacity()).queuePollingTimeout(options.getQueuePollingTimeout()).readers(Arrays.asList(notificationReader, scanReader)).build();
        }

        protected ItemReader<String> keyReader(Mode mode, RedisClient client, RedisOptions redisOptions, Options options, ScanArgs scanArgs) {
            switch (mode) {
                case LIVE:
                    return multiplexingReader(options, notificationReader(client, redisOptions, options), scanReader(client, scanArgs));
                case NEW:
                    return notificationReader(client, redisOptions, options);
                default:
                    return scanReader(client, scanArgs);
            }
        }

        protected ItemReader<String> keyReader(Mode mode, RedisClusterClient client, RedisOptions redisOptions, Options options, ScanArgs scanArgs) {
            switch (mode) {
                case LIVE:
                    return multiplexingReader(options, notificationReader(client, redisOptions, options), scanReader(client, scanArgs));
                case NEW:
                    return notificationReader(client, redisOptions, options);
                default:
                    return scanReader(client, scanArgs);
            }
        }


        private RedisKeyspaceNotificationItemReader<String, String> notificationReader(RedisClient client, RedisOptions redisOptions, Options options) {
            return RedisKeyspaceNotificationItemReader.builder().client(client).patterns(patterns(redisOptions)).queueCapacity(options.getQueueCapacity()).queuePollingTimeout(options.getQueuePollingTimeout()).build();
        }

        private RedisClusterKeyspaceNotificationItemReader<String, String> notificationReader(RedisClusterClient client, RedisOptions redisOptions, Options options) {
            return RedisClusterKeyspaceNotificationItemReader.builder().client(client).patterns(patterns(redisOptions)).queueCapacity(options.getQueueCapacity()).queuePollingTimeout(options.getQueuePollingTimeout()).build();
        }

        private RedisScanItemReader<String, String> scanReader(RedisClient client, ScanArgs options) {
            return new RedisScanItemReader<>(client.connect(), options);
        }


        private RedisClusterScanItemReader<String, String> scanReader(RedisClusterClient client, ScanArgs options) {
            return new RedisClusterScanItemReader<>(client.connect(), options);
        }

    }

}
