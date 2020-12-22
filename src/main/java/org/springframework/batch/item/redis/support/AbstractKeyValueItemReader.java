package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public abstract class AbstractKeyValueItemReader<K, V, T extends KeyValue<K, ?>, C extends StatefulConnection<K, V>> extends AbstractPollableItemReader<T> implements ValueReader<K, T> {

    private final int threads;
    private final int chunkSize;
    private final long commandTimeout;
    @Getter
    private final ItemReader<K> keyReader;
    private final ValueReader valueReader;
    private JobExecution jobExecution;

    protected AbstractKeyValueItemReader(ItemReader<K> keyReader, Duration commandTimeout, int chunkSize, int threads, int queueCapacity, Duration pollingTimeout) {
        super(pollingTimeout);
        Assert.notNull(keyReader, "Key reader is required.");
        Assert.notNull(commandTimeout, "Command timeout is required.");
        Assert.isTrue(chunkSize > 0, "Chunk size must be greater than zero.");
        Assert.isTrue(threads > 0, "Thread count must be greater than zero.");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero.");
        this.keyReader = keyReader;
        this.commandTimeout = commandTimeout.getSeconds();
        this.threads = threads;
        this.chunkSize = chunkSize;
        this.valueReader = new ValueReader(queueCapacity);
    }

    @Override
    protected void doOpen() throws Exception {
        JobFactory factory = new JobFactory();
        factory.afterPropertiesSet();
        String name = ClassUtils.getShortName(getClass());
        TaskletStep step = factory.<K, K>step(name + "-step", chunkSize, threads).reader(keyReader).writer(valueReader).build();
        Job job = factory.getJobBuilderFactory().get(name + "-job").start(step).build();
        this.jobExecution = factory.executeAsync(job, new JobParameters());
        while (!jobExecution.isRunning()) {
            Thread.sleep(1);
        }
    }

    public boolean isRunning() {
        if (jobExecution == null) {
            return false;
        }
        return jobExecution.isRunning();
    }

    @Override
    public boolean isTerminated() {
        return !isRunning();
    }

    @Override
    protected void doClose() {
        if (isRunning()) {
            log.warn("Enqueuer job still running");
        }
    }

    private class ValueReader extends AbstractItemStreamItemWriter<K> {

        @Getter
        private final BlockingQueue<T> queue;

        public ValueReader(int queueCapacity) {
            queue = new LinkedBlockingDeque<>(queueCapacity);
        }

        @Override
        public void open(ExecutionContext executionContext) {
            MetricsUtils.createGaugeCollectionSize("reader.queue.size", queue);
            super.open(executionContext);
        }

        @Override
        public void close() {
            super.close();
            if (!queue.isEmpty()) {
                log.warn("Closing {} with {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
            }
        }

        @Override
        public void write(List<? extends K> items) throws Exception {
            for (T value : values(items)) {
                queue.removeIf(v -> v.getKey().equals(value.getKey()));
                queue.put(value);
            }
        }

    }

    @Override
    public List<T> values(List<? extends K> keys) throws Exception {
        try (C connection = connection()) {
            BaseRedisAsyncCommands<K, V> commands = commands(connection);
            commands.setAutoFlushCommands(false);
            try {
                return readValues(keys, commands, commandTimeout);
            } finally {
                commands.setAutoFlushCommands(true);
            }
        }
    }

    protected abstract C connection() throws Exception;

    protected abstract BaseRedisAsyncCommands<K, V> commands(C connection);

    protected abstract List<T> readValues(List<? extends K> keys, BaseRedisAsyncCommands<K, V> commands, long timeout) throws InterruptedException, ExecutionException, TimeoutException;

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return valueReader.getQueue().poll(timeout, unit);
    }

    private static class KeyValueItemReaderBuilder<B extends KeyValueItemReaderBuilder<B>> extends CommandTimeoutBuilder<B> {

        public static final Duration DEFAULT_POLLING_TIMEOUT = Duration.ofMillis(100);
        public static final int DEFAULT_QUEUE_CAPACITY = 1000;
        public static final int DEFAULT_CHUNK_SIZE = 50;
        public static final int DEFAULT_THREAD_COUNT = 1;
        public static final String DEFAULT_KEY_PATTERN = "*";

        protected int chunkSize = DEFAULT_CHUNK_SIZE;
        protected int threads = DEFAULT_THREAD_COUNT;
        protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        protected Duration pollingTimeout = DEFAULT_POLLING_TIMEOUT;

        protected String keyPattern = DEFAULT_KEY_PATTERN;

        public B keyPattern(String keyPattern) {
            this.keyPattern = keyPattern;
            return (B) this;
        }

        public B chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return (B) this;
        }

        public B threads(int threads) {
            this.threads = threads;
            return (B) this;
        }

        public B queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return (B) this;
        }

        public B pollingTimeout(Duration pollingTimeout) {
            this.pollingTimeout = pollingTimeout;
            return (B) this;
        }

    }

    protected static abstract class ScanKeyValueItemReaderBuilder<B extends ScanKeyValueItemReaderBuilder<B>> extends KeyValueItemReaderBuilder<B> {

        public static final long DEFAULT_SCAN_COUNT = 1000;
        public static final int DEFAULT_SAMPLE_SIZE = 30;

        protected long scanCount = DEFAULT_SCAN_COUNT;

        public B scanCount(long scanCount) {
            this.scanCount = scanCount;
            return (B) this;
        }

    }


    protected static abstract class NotificationKeyValueItemReaderBuilder<B extends NotificationKeyValueItemReaderBuilder<B>> extends KeyValueItemReaderBuilder<B> {

        public static final int DEFAULT_DATABASE = 0;
        protected static final Converter<String, String> DEFAULT_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);
        private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

        private static final int DEFAULT_QUEUE_CAPACITY = 1000;
        private static final Duration DEFAULT_QUEUE_POLLING_TIMEOUT = Duration.ofMillis(100);

        private int database = DEFAULT_DATABASE;
        protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        protected Duration queuePollingTimeout = DEFAULT_QUEUE_POLLING_TIMEOUT;

        public B queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return (B) this;
        }

        public B queuePollingTimeout(Duration queuePollingTimeout) {
            this.queuePollingTimeout = queuePollingTimeout;
            return (B) this;
        }

        public B database(int database) {
            this.database = database;
            return (B) this;
        }

        protected String pubSubPattern() {
            return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
        }

    }


}
