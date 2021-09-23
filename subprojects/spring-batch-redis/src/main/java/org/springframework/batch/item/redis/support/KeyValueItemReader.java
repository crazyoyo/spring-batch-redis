package org.springframework.batch.item.redis.support;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.codec.StringCodec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class KeyValueItemReader<T extends KeyValue<?>> extends AbstractItemStreamItemReader<T> {

    private final ItemReader<String> keyReader;
    private final ItemProcessor<List<? extends String>, List<T>> valueReader;
    private final int threads;
    private final int chunkSize;
    private final int queueCapacity;
    private final Duration queuePollTimeout;
    private final SkipPolicy skipPolicy;

    protected BlockingQueue<T> queue;
    private long pollTimeout;
    private JobExecution jobExecution;
    private String name;


    public KeyValueItemReader(ItemReader<String> keyReader, ItemProcessor<List<? extends String>, List<T>> valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout, SkipPolicy skipPolicy) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required");
        Assert.notNull(valueReader, "A value reader is required");
        Assert.isTrue(threads > 0, "Thread count must be greater than zero");
        Assert.isTrue(chunkSize > 0, "Chunk size must be greater than zero");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero");
        Assert.notNull(queuePollTimeout, "Queue poll timeout must not be null");
        Assert.isTrue(!queuePollTimeout.isZero(), "Queue poll timeout must not be zero");
        Assert.isTrue(!queuePollTimeout.isNegative(), "Queue poll timeout must not be negative");
        Assert.notNull(skipPolicy, "A skip policy is required");
        this.keyReader = keyReader;
        this.valueReader = valueReader;
        this.threads = threads;
        this.chunkSize = chunkSize;
        this.queueCapacity = queueCapacity;
        this.queuePollTimeout = queuePollTimeout;
        this.skipPolicy = skipPolicy;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void setName(String name) {
        this.name = name;
        super.setName(name);
    }

    @SuppressWarnings("BusyWait")
    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        if (jobExecution != null) {
            log.debug("Already opened, skipping");
            return;
        }
        log.debug("Opening {}", name);
        queue = new LinkedBlockingDeque<>(queueCapacity);
        pollTimeout = queuePollTimeout.toMillis();
        MetricsUtils.createGaugeCollectionSize("reader.queue.size", queue);
        ItemWriter<String> writer = new ValueWriter<>(valueReader, queue);
        JobFactory factory = new JobFactory();
        try {
            factory.afterPropertiesSet();
        } catch (Exception e) {
            throw new ItemStreamException("Failed to initialize the reader", e);
        }
        FaultTolerantStepBuilder<String, String> stepBuilder = faultTolerantStepBuilder(factory.getStepBuilderFactory().get(name + "-step").chunk(chunkSize));
        stepBuilder.skipPolicy(skipPolicy);
        stepBuilder.reader(keyReader).writer(writer);
        if (threads > 1) {
            ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
            taskExecutor.setMaxPoolSize(threads);
            taskExecutor.setCorePoolSize(threads);
            taskExecutor.afterPropertiesSet();
            stepBuilder.taskExecutor(taskExecutor).throttleLimit(threads);
        }
        Job job = factory.getJobBuilderFactory().get(name + "-job").start(stepBuilder.build()).build();
        try {
            this.jobExecution = factory.getAsyncLauncher().run(job, new JobParameters());
        } catch (Exception e) {
            throw new ItemStreamException("Could not run job " + job.getName());
        }
        while (!jobExecution.isRunning()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new ItemStreamException("Interrupted while waiting for job to run");
            }
        }
        super.open(executionContext);
        log.debug("Opened {}", name);
    }

    protected FaultTolerantStepBuilder<String, String> faultTolerantStepBuilder(SimpleStepBuilder<String, String> stepBuilder) {
        return stepBuilder.faultTolerant();
    }

    @Override
    public T read() throws Exception {
        T item;
        do {
            item = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);
        } while (item == null && jobExecution.isRunning());
        return item;
    }

    public List<T> read(int maxElements) {
        List<T> items = new ArrayList<>(maxElements);
        queue.drainTo(items, maxElements);
        return items;
    }

    @SuppressWarnings("BusyWait")
    @Override
    public synchronized void close() {
        if (jobExecution == null) {
            log.debug("Already closed, skipping");
            return;
        }
        log.debug("Closing {}", name);
        super.close();
        if (!queue.isEmpty()) {
            log.warn("Closing {} with {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
        }
        while (jobExecution.isRunning()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new ItemStreamException("Interrupted while waiting for job to finish running");
            }
        }
        queue = null;
        jobExecution = null;
        log.debug("Closed {}", name);
    }

    private static class ValueWriter<T extends KeyValue<?>> extends AbstractItemStreamItemWriter<String> {

        private final ItemProcessor<List<? extends String>, List<T>> valueReader;
        private final BlockingQueue<T> queue;

        private ValueWriter(ItemProcessor<List<? extends String>, List<T>> valueReader, BlockingQueue<T> queue) {
            this.valueReader = valueReader;
            this.queue = queue;
        }

        @Override
        public void open(ExecutionContext executionContext) {
            super.open(executionContext);
            if (valueReader instanceof ItemStream) {
                ((ItemStream) valueReader).open(executionContext);
            }
        }

        @Override
        public void update(ExecutionContext executionContext) {
            super.update(executionContext);
            if (valueReader instanceof ItemStream) {
                ((ItemStream) valueReader).update(executionContext);
            }
        }

        @Override
        public void close() {
            if (valueReader instanceof ItemStream) {
                ((ItemStream) valueReader).close();
            }
            super.close();
        }

        @Override
        public void write(List<? extends String> items) throws Exception {
            List<T> values = valueReader.process(items);
            if (values == null) {
                return;
            }
            for (T value : values) {
                queue.removeIf(v -> v.getKey().equals(value.getKey()));
                queue.put(value);
            }
        }

    }

    @SuppressWarnings("unchecked")
    public static class AbstractKeyValueItemReaderBuilder<T extends KeyValue<?>, R extends ItemProcessor<List<? extends String>, List<T>>, B extends AbstractKeyValueItemReaderBuilder<T, R, B>> extends CommandBuilder<String, String, B> {

        public static final int DEFAULT_THREADS = 1;
        public static final int DEFAULT_CHUNK_SIZE = 50;
        public static final int DEFAULT_QUEUE_CAPACITY = 1000;
        public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);
        public static final Map<Class<? extends Throwable>, Boolean> DEFAULT_SKIPPABLE_EXCEPTIONS = Stream.of(RedisCommandExecutionException.class, RedisCommandTimeoutException.class, TimeoutException.class).collect(Collectors.toMap(t -> t, t -> true));
        public static final int DEFAULT_SKIP_LIMIT = 3;
        public static final SkipPolicy DEFAULT_SKIP_POLICY = new LimitCheckingItemSkipPolicy(DEFAULT_SKIP_LIMIT, DEFAULT_SKIPPABLE_EXCEPTIONS);

        protected final R valueReader;
        protected final AbstractRedisClient client;

        protected int threads = DEFAULT_THREADS;
        protected int chunkSize = DEFAULT_CHUNK_SIZE;
        protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        protected Duration queuePollTimeout = DEFAULT_QUEUE_POLL_TIMEOUT;
        protected SkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

        public AbstractKeyValueItemReaderBuilder(AbstractRedisClient client, R valueReader) {
            super(client, StringCodec.UTF8);
            this.client = client;
            this.valueReader = valueReader;
        }

        public B threads(int threads) {
            Assert.isTrue(threads > 0, "Thread count must be greater than zero");
            this.threads = threads;
            return (B) this;
        }

        public B chunkSize(int chunkSize) {
            Assert.isTrue(chunkSize > 0, "Chunk size must be greater than zero");
            this.chunkSize = chunkSize;
            return (B) this;
        }

        public B queueCapacity(int queueCapacity) {
            Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero");
            this.queueCapacity = queueCapacity;
            return (B) this;
        }

        public B queuePollTimeout(Duration queuePollTimeout) {
            Assert.notNull(queuePollTimeout, "Queue poll timeout must not be null");
            Assert.isTrue(!queuePollTimeout.isZero(), "Queue poll timeout must not be zero");
            Assert.isTrue(!queuePollTimeout.isNegative(), "Queue poll timeout must not be negative");
            this.queuePollTimeout = queuePollTimeout;
            return (B) this;
        }

        public B skipPolicy(SkipPolicy skipPolicy) {
            Assert.notNull(skipPolicy, "Skip policy must not be null");
            this.skipPolicy = skipPolicy;
            return (B) this;
        }

    }


    @SuppressWarnings("unchecked")
    public static class KeyValueItemReaderBuilder<T extends KeyValue<?>, R extends ItemProcessor<List<? extends String>, List<T>>, B extends KeyValueItemReaderBuilder<T, R, B>> extends AbstractKeyValueItemReaderBuilder<T, R, B> {

        public static final String DEFAULT_SCAN_MATCH = "*";
        public static final long DEFAULT_SCAN_COUNT = 1000;

        private String scanMatch = DEFAULT_SCAN_MATCH;
        private long scanCount = DEFAULT_SCAN_COUNT;
        private String scanType;

        public B scanMatch(String scanMatch) {
            this.scanMatch = scanMatch;
            return (B) this;
        }

        public B scanCount(long scanCount) {
            this.scanCount = scanCount;
            return (B) this;
        }

        public B scanType(String scanType) {
            this.scanType = scanType;
            return (B) this;
        }

        protected KeyValueItemReaderBuilder(AbstractRedisClient client, R valueReader) {
            super(client, valueReader);
        }

        public ItemReader<String> keyReader() {
            return new ScanKeyItemReader(connectionSupplier(), sync(), scanMatch, scanCount, scanType);
        }
    }

    @SuppressWarnings("unchecked")
    public static class LiveKeyValueItemReaderBuilder<T extends KeyValue<?>, R extends ItemProcessor<List<? extends String>, List<T>>, B extends LiveKeyValueItemReaderBuilder<T, R, B>> extends AbstractKeyValueItemReaderBuilder<T, R, B> {

        public static final int DEFAULT_QUEUE_CAPACITY = 1000;
        public static final int DEFAULT_DATABASE = 0;
        public static final String DEFAULT_KEY_PATTERN = "*";
        public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";
        public static final List<String> DEFAULT_PUBSUB_PATTERNS = pubSubPatterns(DEFAULT_DATABASE, DEFAULT_KEY_PATTERN);

        protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private String[] keyPatterns = new String[]{DEFAULT_KEY_PATTERN};
        private int database = DEFAULT_DATABASE;
        protected Duration flushingInterval = FlushingStepBuilder.DEFAULT_FLUSHING_INTERVAL;
        protected Duration idleTimeout;

        public B queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return (B) this;
        }

        public B flushingInterval(Duration flushingInterval) {
            this.flushingInterval = flushingInterval;
            return (B) this;
        }

        public B idleTimeout(Duration idleTimeout) {
            this.idleTimeout = idleTimeout;
            return (B) this;
        }

        public B database(int database) {
            this.database = database;
            return (B) this;
        }

        public B keyPatterns(String... keyPatterns) {
            this.keyPatterns = keyPatterns;
            return (B) this;
        }

        protected LiveKeyValueItemReaderBuilder(AbstractRedisClient client, R valueReader) {
            super(client, valueReader);
        }

        public static List<String> pubSubPatterns(int database, String... keyPatterns) {
            List<String> patterns = new ArrayList<>();
            for (String keyPattern : keyPatterns) {
                patterns.add(pubSubPattern(database, keyPattern));
            }
            return patterns;
        }

        public static String pubSubPattern(int database, String keyPattern) {
            return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
        }

        @SuppressWarnings("rawtypes")
        public PollableItemReader<String> keyReader() {
            if (client instanceof RedisModulesClusterClient) {
                return new RedisClusterKeyspaceNotificationItemReader((Supplier) pubSubConnectionSupplier(), pubSubPatterns(database, keyPatterns), queueCapacity);
            }
            return new RedisKeyspaceNotificationItemReader(pubSubConnectionSupplier(), pubSubPatterns(database, keyPatterns), queueCapacity);
        }
    }

}
