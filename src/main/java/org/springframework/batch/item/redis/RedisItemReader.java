package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.text.StringSubstitutor;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class RedisItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> implements InitializingBean {

    @Getter
    private final ItemReader<K> keyReader;
    private final ItemProcessor<List<K>, List<T>> valueReader;
    private final int batchSize;
    private final int queueCapacity;
    private final long maxWait;
    private final int nThreads;

    private BlockingQueue<T> valueQueue;
    private ExecutorService executor;
    private List<KeyProcessorThread> threads;

    public RedisItemReader(ItemReader<K> keyReader, ItemProcessor<List<K>, List<T>> valueReader, int batchSize, int queueCapacity, long maxWait, int nThreads) {
        setName(ClassUtils.getShortName(getClass()));
        this.keyReader = keyReader;
        this.valueReader = valueReader;
        this.batchSize = batchSize;
        this.queueCapacity = queueCapacity;
        this.maxWait = maxWait;
        this.nThreads = nThreads;
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(valueReader, "A key processor is required.");
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
        threads = new ArrayList<>(nThreads);
        executor = Executors.newFixedThreadPool(nThreads);
        for (int index = 0; index < nThreads; index++) {
            KeyProcessorThread thread = new KeyProcessorThread();
            threads.add(thread);
            executor.submit(thread);
        }
        executor.shutdown();
    }

    public void flush() {
        for (KeyProcessorThread thread : threads) {
            thread.flush();
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
        for (KeyProcessorThread thread : threads) {
            thread.stop();
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
            value = valueQueue.poll(maxWait, TimeUnit.MILLISECONDS);
        } while (value == null && !executor.isTerminated());
        return value;
    }

    private class KeyProcessorThread implements Runnable {

        private final List<K> keys = new ArrayList<>(batchSize);
        private boolean stopped;

        public void stop() {
            this.stopped = true;
        }

        @Override
        public void run() {
            K key;
            try {
                while ((key = keyReader.read()) != null && !stopped) {
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

    public static RedisItemReaderBuilder builder() {
        return new RedisItemReaderBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisItemReaderBuilder extends RedisBuilder {
        public static final int DEFAULT_BATCH_SIZE = 50;
        public static final int DEFAULT_QUEUE_CAPACITY = 10000;
        public static final int DEFAULT_THREADS = 1;
        public static final long DEFAULT_MAX_WAIT = 50;
        public static final long DEFAULT_QUEUE_POLLING_TIMEOUT = 100;
        public static final String DATABASE_TOKEN = "database";
        public static final String KEYSPACE_CHANNEL_TEMPLATE = "__keyspace@${" + DATABASE_TOKEN + "}__:*";
        private static final BiFunction<String, String, String> KEY_EXTRACTOR = (c, m) -> c.substring(c.indexOf(":") + 1);

        private RedisURI redisURI;
        private boolean cluster;
        private ScanArgs scanArgs = new ScanArgs();
        private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private long maxWait = DEFAULT_MAX_WAIT;
        private int threads = DEFAULT_THREADS;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private long queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;
        private String[] keyspacePatterns;

        private void validate() {
            Assert.notNull(redisURI, "A RedisURI instance is required.");
        }

        private GenericObjectPool<StatefulRedisClusterConnection<String, String>> connectionPool(RedisClusterClient client) {
            return connectionPool(client, maxTotal, minIdle, maxIdle);
        }

        private GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool(RedisClient client) {
            return connectionPool(client, maxTotal, minIdle, maxIdle);
        }

        private <T> RedisItemReader<String, T> reader(ItemReader<String> keyReader, ItemProcessor<List<String>, List<T>> valueProcessor) {
            return new RedisItemReader<>(keyReader, valueProcessor, batchSize, queueCapacity, maxWait, threads);
        }

        public RedisItemReader<String, KeyValue<String>> valueReader() {
            if (cluster) {
                RedisClusterClient client = RedisClusterClient.create(redisURI);
                return reader(scanReader(client.connect(), StatefulRedisClusterConnection::sync), keyValueProcessor(connectionPool(client), StatefulRedisClusterConnection::async));
            }
            RedisClient client = RedisClient.create(redisURI);
            return reader(scanReader(client.connect(), StatefulRedisConnection::sync), keyValueProcessor(connectionPool(client), StatefulRedisConnection::async));
        }

        public RedisItemReader<String, KeyValue<String>> continuousValueReader() {
            if (cluster) {
                RedisClusterClient client = RedisClusterClient.create(redisURI);
                return reader(continuousKeyReader(client), keyValueProcessor(connectionPool(client), StatefulRedisClusterConnection::async));
            }
            RedisClient client = RedisClient.create(redisURI);
            return reader(continuousKeyReader(client), keyValueProcessor(connectionPool(client), StatefulRedisConnection::async));
        }

        private ItemReader<String> continuousKeyReader(RedisClient client) {
            return multiplexingReader(keyspaceNotificationReader(client), scanReader(client.connect(), StatefulRedisConnection::sync));
        }

        private ItemReader<String> continuousKeyReader(RedisClusterClient client) {
            return multiplexingReader(keyspaceNotificationReader(client), scanReader(client.connect(), StatefulRedisClusterConnection::sync));
        }

        private ItemReader<String> multiplexingReader(ItemReader<String>... readers) {
            return MultiplexingItemReader.<String>builder().readers(Arrays.asList(readers)).queue(queue()).pollingTimeout(queuePollingTimeout).build();
        }

        public RedisItemReader<String, KeyDump<String>> dumpReader() {
            if (cluster) {
                RedisClusterClient client = RedisClusterClient.create(redisURI);
                return reader(scanReader(client.connect(), StatefulRedisClusterConnection::sync), keyDumpProcessor(connectionPool(client), StatefulRedisClusterConnection::async));
            }
            RedisClient client = RedisClient.create(redisURI);
            return reader(scanReader(client.connect(), StatefulRedisConnection::sync), keyDumpProcessor(connectionPool(client), StatefulRedisConnection::async));
        }

        public RedisItemReader<String, KeyDump<String>> continuousDumpReader() {
            if (cluster) {
                RedisClusterClient client = RedisClusterClient.create(redisURI);
                return reader(continuousKeyReader(client), keyDumpProcessor(connectionPool(client), StatefulRedisClusterConnection::async));

            }
            RedisClient client = RedisClient.create(redisURI);
            return reader(continuousKeyReader(client), keyDumpProcessor(connectionPool(client), StatefulRedisConnection::async));
        }

        private <C extends StatefulConnection<String, String>> ItemProcessor<List<String>, List<KeyValue<String>>> keyValueProcessor(GenericObjectPool<C> connectionPool, Function<C, BaseRedisAsyncCommands<String, String>> commands) {
            return KeyValueItemProcessor.<String, String, C>builder().pool(connectionPool).commands(commands).commandTimeout(commandTimeout).build();
        }

        private <C extends StatefulConnection<String, String>> ItemProcessor<List<String>, List<KeyDump<String>>> keyDumpProcessor(GenericObjectPool<C> connectionPool, Function<C, BaseRedisAsyncCommands<String, String>> commands) {
            return KeyDumpItemProcessor.<String, String, C>builder().pool(connectionPool).commands(commands).commandTimeout(commandTimeout).build();
        }

        private ItemReader<String> keyspaceNotificationReader(RedisClient client) {
            return RedisKeyspaceNotificationItemReader.<String, String>builder().connection(client.connectPubSub()).queue(queue()).pollingTimeout(queuePollingTimeout).patterns(keyspacePatterns()).keyExtractor(KEY_EXTRACTOR).build();
        }

        private ItemReader<String> keyspaceNotificationReader(RedisClusterClient client) {
            return RedisClusterKeyspaceNotificationItemReader.<String, String>builder().connection(client.connectPubSub()).queue(queue()).pollingTimeout(queuePollingTimeout).patterns(keyspacePatterns()).keyExtractor(KEY_EXTRACTOR).build();
        }

        private <C extends StatefulConnection<String, String>> ItemReader<String> scanReader(C connection, Function<C, RedisKeyCommands<String, String>> commands) {
            return ScanItemReader.<String, String, C>builder().connection(connection).commands(commands).scanArgs(scanArgs).build();
        }

        private String[] keyspacePatterns() {
            if (keyspacePatterns == null) {
                Map<String, String> variables = new HashMap<>();
                variables.put(DATABASE_TOKEN, String.valueOf(redisURI.getDatabase()));
                StringSubstitutor substitutor = new StringSubstitutor(variables);
                return new String[]{substitutor.replace(KEYSPACE_CHANNEL_TEMPLATE)};
            }
            return keyspacePatterns;
        }

        private <T> BlockingQueue<T> queue() {
            return new LinkedBlockingDeque<>(queueCapacity);
        }
    }

}
