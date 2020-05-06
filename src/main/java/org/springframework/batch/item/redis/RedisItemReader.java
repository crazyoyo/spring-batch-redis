package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> {

    @Builder(builderMethodName = "dump", builderClassName = "RedisDumpItemReaderBuilder")
    private static RedisItemReader<String, KeyDump<String>> redisDumpItemReader(RedisClient client, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait) {
        return RedisItemReader.<String, KeyDump<String>>builder().keyReader(RedisScanItemReader.<String, String>builder().connection(client.connect()).count(scanCount).match(scanMatch).build()).valueReader(RedisDumpReader.<String, String>builder().pool(pool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).build();
    }

    @Builder(builderMethodName = "liveDump", builderClassName = "RedisLiveDumpItemReaderBuilder")
    private static RedisItemReader<String, KeyDump<String>> redisLiveDumpItemReader(RedisClient client, Integer database, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait, Duration flushPeriod) {
        return RedisItemReader.<String, KeyDump<String>>builder().keyReader(liveKeyReader().client(client).database(database).scanCount(scanCount).scanMatch(scanMatch).queueCapacity(queueCapacity).build()).valueReader(RedisDumpReader.<String, String>builder().pool(pool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).flushPeriod(flushPeriod).build();
    }

    @Builder(builderMethodName = "value", builderClassName = "RedisValueItemReaderBuilder")
    private static RedisItemReader<String, KeyValue<String>> redisValueItemReader(RedisClient client, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait) {
        return RedisItemReader.<String, KeyValue<String>>builder().keyReader(RedisScanItemReader.<String, String>builder().connection(client.connect()).count(scanCount).match(scanMatch).build()).valueReader(RedisValueReader.<String, String>builder().pool(pool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).build();
    }

    @Builder(builderMethodName = "liveValue", builderClassName = "RedisLiveValueItemReaderBuilder")
    private static RedisItemReader<String, KeyValue<String>> redisLiveValueItemReader(RedisClient client, Integer database, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait, Duration flushPeriod) {
        return RedisItemReader.<String, KeyValue<String>>builder().keyReader(liveKeyReader().client(client).database(database).scanCount(scanCount).scanMatch(scanMatch).queueCapacity(queueCapacity).build()).valueReader(RedisValueReader.<String, String>builder().pool(pool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).flushPeriod(flushPeriod).build();
    }

    @Builder(builderMethodName = "liveKeyReader", builderClassName = "RedisLiveKeyItemReaderBuilder")
    private static MultiplexingItemReader<String> redisLiveKeyItemReader(RedisClient client, Integer database, Long scanCount, String scanMatch, Integer queueCapacity) {
        return MultiplexingItemReader.<String>builder().queueCapacity(queueCapacity).readers(RedisKeyspaceNotificationItemReader.<String>builder().connection(client.connectPubSub()).database(database).build(), RedisScanItemReader.<String, String>builder().connection(client.connect()).count(scanCount).match(scanMatch).build()).build();
    }

    @Builder(builderMethodName = "clusterDump", builderClassName = "RedisClusterDumpItemReaderBuilder")
    private static RedisItemReader<String, KeyDump<String>> redisClusterDumpItemReader(RedisClusterClient client, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait) {
        return RedisItemReader.<String, KeyDump<String>>builder().keyReader(RedisClusterScanItemReader.<String, String>builder().connection(client.connect()).count(scanCount).match(scanMatch).build()).valueReader(RedisClusterDumpReader.<String, String>builder().pool(clusterPool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).build();
    }

    @Builder(builderMethodName = "clusterLiveDump", builderClassName = "RedisClusterLiveDumpItemReaderBuilder")
    private static RedisItemReader<String, KeyDump<String>> redisClusterLiveDumpItemReader(RedisClusterClient client, Integer database, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait, Duration flushPeriod) {
        return RedisItemReader.<String, KeyDump<String>>builder().keyReader(clusterLiveKeyReader().client(client).database(database).scanCount(scanCount).scanMatch(scanMatch).queueCapacity(queueCapacity).build()).valueReader(RedisClusterDumpReader.<String, String>builder().pool(clusterPool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).flushPeriod(flushPeriod).build();
    }

    @Builder(builderMethodName = "clusterValue", builderClassName = "RedisClusterValueItemReaderBuilder")
    private static RedisItemReader<String, KeyValue<String>> redisClusterValueItemReader(RedisClusterClient client, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait) {
        return RedisItemReader.<String, KeyValue<String>>builder().keyReader(RedisClusterScanItemReader.<String, String>builder().connection(client.connect()).count(scanCount).match(scanMatch).build()).valueReader(RedisClusterValueReader.<String, String>builder().pool(clusterPool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).build();
    }

    @Builder(builderMethodName = "clusterLiveValue", builderClassName = "RedisClusterLiveValueItemReaderBuilder")
    private static RedisItemReader<String, KeyValue<String>> redisClusterLiveValueItemReader(RedisClusterClient client, Integer database, Long scanCount, String scanMatch, Duration timeout, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait, Duration flushPeriod) {
        return RedisItemReader.<String, KeyValue<String>>builder().keyReader(clusterLiveKeyReader().client(client).database(database).scanCount(scanCount).scanMatch(scanMatch).queueCapacity(queueCapacity).build()).valueReader(RedisClusterValueReader.<String, String>builder().pool(clusterPool(client, threads)).timeout(timeout).build()).batchSize(batchSize).threads(threads).queueCapacity(queueCapacity).maxWait(maxWait).flushPeriod(flushPeriod).build();
    }

    @Builder(builderMethodName = "clusterLiveKeyReader", builderClassName = "RedisClusterLiveKeyItemReaderBuilder")
    private static MultiplexingItemReader<String> redisClusterLiveKeyReader(RedisClusterClient client, Integer database, Long scanCount, String scanMatch, Integer queueCapacity) {
        return MultiplexingItemReader.<String>builder().queueCapacity(queueCapacity).readers(RedisClusterKeyspaceNotificationItemReader.<String>builder().connection(client.connectPubSub()).database(database).build(), RedisClusterScanItemReader.<String, String>builder().connection(client.connect()).count(scanCount).match(scanMatch).build()).build();
    }

    private static GenericObjectPool<StatefulRedisConnection<String, String>> pool(RedisClient client, Integer threads) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(threads));
    }

    private static GenericObjectPool<StatefulRedisClusterConnection<String, String>> clusterPool(RedisClusterClient client, Integer threads) {
        return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(threads));
    }

    private static <T> GenericObjectPoolConfig<T> poolConfig(Integer threads) {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(threads == null ? DEFAULT_THREADS : threads);
        return config;
    }

    public final static int DEFAULT_BATCH_SIZE = 50;
    public final static long DEFAULT_MAX_WAIT = 50;
    public static final int DEFAULT_QUEUE_CAPACITY = 10000;
    public static final int DEFAULT_THREADS = 1;

    @Getter
    @Setter
    private int batchSize;
    @Getter
    @Setter
    private int queueCapacity;
    @Getter
    @Setter
    private long maxWait;
    @Getter
    @Setter
    private ItemReader<K> keyReader;
    @Getter
    @Setter
    private int threads;
    @Getter
    @Setter
    private Duration flushPeriod;
    @Getter
    @Setter
    private ItemProcessor<List<K>, List<T>> valueReader;

    private BlockingQueue<T> valueQueue;
    private ExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Builder
    private RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<K>, List<T>> valueReader, Integer batchSize, Integer threads, Integer queueCapacity, Long maxWait, Duration flushPeriod) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(valueReader, "A key processor is required.");
        this.keyReader = keyReader;
        this.valueReader = valueReader;
        this.batchSize = batchSize == null ? DEFAULT_BATCH_SIZE : batchSize;
        this.queueCapacity = queueCapacity == null ? DEFAULT_QUEUE_CAPACITY : queueCapacity;
        this.maxWait = maxWait == null ? DEFAULT_MAX_WAIT : maxWait;
        this.threads = threads == null ? DEFAULT_THREADS : threads;
        this.flushPeriod = flushPeriod;
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
        valueQueue = new LinkedBlockingDeque<>(queueCapacity);
        executor = Executors.newFixedThreadPool(threads);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        for (int index = 0; index < threads; index++) {
            KeyProcessorThread threads = new KeyProcessorThread();
            if (flushPeriod != null) {
                scheduler.scheduleAtFixedRate(threads::flush, flushPeriod.toMillis(), flushPeriod.toMillis(), TimeUnit.MILLISECONDS);
            }
            executor.submit(threads);
        }
        executor.shutdown();
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
        scheduler.shutdown();
        scheduler = null;
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
            value = valueQueue.poll(maxWait, TimeUnit.MILLISECONDS);
        } while (value == null && !executor.isTerminated());
        return value;
    }

    private class KeyProcessorThread implements Runnable {

        private final List<K> keys = new ArrayList<>(batchSize);

        @Override
        public void run() {
            K key;
            try {
                while ((key = keyReader.read()) != null) {
                    addKey(key);
                }
            } catch (Exception e) {
                log.error("Could not read key", e);
            }
            flush();
        }

        private void addKey(K key) {
            synchronized (keys) {
                keys.add(key);
            }
            if (keys.size() >= batchSize) {
                flush();
            }
        }

        public void flush() {
            List<K> snapshot;
            synchronized (keys) {
                snapshot = new ArrayList<>(keys);
                keys.clear();
            }
            List<T> values;
            try {
                values = valueReader.process(snapshot);
            } catch (Exception e) {
                log.error("Could not get {} values", snapshot.size(), e);
                return;
            }
            if (values != null) {
                valueQueue.addAll(values);
            }
        }

    }

}
