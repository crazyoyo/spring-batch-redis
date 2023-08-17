package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.reader.KeyValueItemProcessor;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.FilteringItemProcessor;
import com.redis.spring.batch.util.Helper;
import com.redis.spring.batch.writer.ProcessingItemWriter;
import com.redis.spring.batch.writer.QueueItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemReader<K, V> extends AbstractItemStreamItemReader<KeyValue<K>>
        implements PollableItemReader<KeyValue<K>> {

    public enum Mode {
        SCAN, LIVE
    }

    public static final int DEFAULT_MEMORY_USAGE_SAMPLES = 5;

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    public static final ValueType DEFAULT_VALUE_TYPE = ValueType.DUMP;

    public static final int DEFAULT_THREADS = 1;

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final String MATCH_ALL = "*";

    public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

    public static final int DEFAULT_DATABASE = 0;

    public static final String DEFAULT_PUBSUB_PATTERN = pattern(DEFAULT_DATABASE, MATCH_ALL);

    public static final OrderingStrategy DEFAULT_ORDERING = OrderingStrategy.PRIORITY;

    public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    public static final Duration DEFAULT_FLUSHING_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;

    public static final Duration DEFAULT_IDLE_TIMEOUT = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private final BlockedKeyItemWriter blockedKeyWriter;

    private final FilteringItemProcessor<K> blockedKeyFilter;

    private ValueType valueType = DEFAULT_VALUE_TYPE;

    private Mode mode = Mode.SCAN;

    private ItemProcessor<K, K> keyProcessor;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private int poolSize = DEFAULT_POOL_SIZE;

    private ReadFrom readFrom;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private DataSize memoryUsageLimit;

    private int memoryUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

    private String scanMatch;

    private String scanType;

    private Long scanCount;

    private int database = DEFAULT_DATABASE;

    private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

    private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;

    private Duration idleTimeout = DEFAULT_IDLE_TIMEOUT;

    private JobRepository jobRepository;

    private JobBuilderFactory jobBuilderFactory;

    private String name;

    private JobExecution jobExecution;

    private BlockingQueue<KeyValue<K>> queue;

    public RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        setName(ClassUtils.getShortName(getClass()));
        this.client = client;
        this.codec = codec;
        this.blockedKeyWriter = new BlockedKeyItemWriter(codec);
        this.blockedKeyFilter = new FilteringItemProcessor<>(compose(Helper.toStringKeyFunction(codec),
                ((Predicate<String>) blockedKeyWriter.getBlockedKeys()::contains).negate()));
    }

    private static <S, T> Predicate<S> compose(Function<S, T> function, Predicate<T> predicate) {
        return s -> predicate.test(function.apply(s));
    }

    private static String pattern(int database, String match) {
        return String.format(PUBSUB_PATTERN_FORMAT, database, match == null ? MATCH_ALL : match);
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public JobRepository getJobRepository() {
        return jobRepository;
    }

    public void setJobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public void setKeyProcessor(ItemProcessor<K, K> processor) {
        this.keyProcessor = processor;
    }

    public ItemProcessor<K, K> getKeyProcessor() {
        return keyProcessor;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getNotificationQueueCapacity() {
        return notificationQueueCapacity;
    }

    public void setNotificationQueueCapacity(int notificationQueueCapacity) {
        this.notificationQueueCapacity = notificationQueueCapacity;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public DataSize getMemoryUsageLimit() {
        return memoryUsageLimit;
    }

    public void setMemoryUsageLimit(DataSize memoryUsageLimit) {
        this.memoryUsageLimit = memoryUsageLimit;
    }

    public int getMemoryUsageSamples() {
        return memoryUsageSamples;
    }

    public void setMemoryUsageSamples(int memoryUsageSamples) {
        this.memoryUsageSamples = memoryUsageSamples;
    }

    public String getScanMatch() {
        return scanMatch;
    }

    public void setScanMatch(String scanMatch) {
        this.scanMatch = scanMatch;
    }

    public String getScanType() {
        return scanType;
    }

    public void setScanType(String scanType) {
        this.scanType = scanType;
    }

    public void setScanCount(Long count) {
        this.scanCount = count;
    }

    public Long getScanCount() {
        return scanCount;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Duration getFlushingInterval() {
        return flushingInterval;
    }

    public void setFlushingInterval(Duration interval) {
        this.flushingInterval = interval;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public OrderingStrategy getOrderingStrategy() {
        return orderingStrategy;
    }

    public void setOrderingStrategy(OrderingStrategy orderingStrategy) {
        this.orderingStrategy = orderingStrategy;
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        this.name = name;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (!isOpen()) {
            doOpen();
        }
    }

    private void doOpen() {
        SimpleStepBuilder<K, K> step = new StepBuilder(name).repository(jobRepository())
                .transactionManager(transactionManager()).chunk(chunkSize);
        ItemReader<K> reader = reader();
        step.reader(reader);
        step.processor(processor());
        step.writer(writer());
        if (threads > 1) {
            step.taskExecutor(Helper.threadPoolTaskExecutor(threads));
            step.throttleLimit(threads);
        }
        if (mode == Mode.LIVE) {
            step = new FlushingStepBuilder<>(step).interval(flushingInterval).idleTimeout(idleTimeout);
        }
        Job job = jobBuilderFactory().get(name).start(step.build()).build();
        try {
            jobExecution = jobLauncher().run(job, new JobParameters());
        } catch (JobExecutionException e) {
            throw new ItemStreamException("Job execution failed", e);
        }
        while (!(Helper.isOpen(reader) || jobExecution.getStatus().isUnsuccessful()
                || jobExecution.getStatus().isLessThanOrEqualTo(BatchStatus.COMPLETED))) {
            sleep();
        }
        if (jobExecution.getStatus().isUnsuccessful()) {
            throw new ItemStreamException("Could not run job", jobExecution.getAllFailureExceptions().iterator().next());
        }
    }

    private ItemReader<K> reader() {
        if (mode == Mode.LIVE) {
            return keyspaceNotificationReader();
        }
        return scanKeyReader();
    }

    private ItemReader<K> keyspaceNotificationReader() {
        KeyspaceNotificationItemReader<K, V> notificationReader = new KeyspaceNotificationItemReader<>(client, codec);
        notificationReader.setKeyType(scanType);
        notificationReader.setOrderingStrategy(orderingStrategy);
        notificationReader.setQueueCapacity(notificationQueueCapacity);
        notificationReader.setPollTimeout(pollTimeout);
        notificationReader.setPattern(pattern(database, scanMatch));
        return notificationReader;
    }

    @SuppressWarnings("unchecked")
    private ItemReader<K> scanKeyReader() {
        StatefulRedisModulesConnection<K, V> connection = RedisModulesUtils.connection(client, codec);
        if (readFrom != null && connection instanceof StatefulRedisClusterConnection) {
            ((StatefulRedisClusterConnection<K, V>) connection).setReadFrom(readFrom);
        }
        KeyScanArgs args = new KeyScanArgs();
        if (scanCount != null) {
            args.limit(scanCount);
        }
        if (scanMatch != null) {
            args.match(scanMatch);
        }
        if (scanType != null) {
            args.type(scanType);
        }
        ScanIterator<K> iterator = ScanIterator.scan(Helper.sync(connection), args);
        return new IteratorItemReader<>(iterator);
    }

    public Set<String> getBlockedKeys() {
        return blockedKeyWriter.getBlockedKeys();
    }

    private JobBuilderFactory jobBuilderFactory() {
        if (jobBuilderFactory == null) {
            jobBuilderFactory = new JobBuilderFactory(jobRepository());
        }
        return jobBuilderFactory;
    }

    private JobRepository jobRepository() {
        if (jobRepository == null) {
            try {
                jobRepository = Helper.inMemoryJobRepository();
            } catch (Exception e) {
                throw new ItemStreamException("Could not initialize job repository", e);
            }
        }
        return jobRepository;
    }

    private SimpleJobLauncher jobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository());
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return jobLauncher;
    }

    private void sleep() {
        try {
            Thread.sleep(pollTimeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ItemStreamException("Interrupted during initialization", e);
        }
    }

    private PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    public JobExecution getJobExecution() {
        return jobExecution;
    }

    private ItemProcessor<? super K, ? extends K> processor() {
        if (shouldBlockKeys()) {
            if (keyProcessor == null) {
                return blockedKeyFilter;
            }
            CompositeItemProcessor<K, K> processor = new CompositeItemProcessor<>();
            processor.setDelegates(Arrays.asList(blockedKeyFilter, keyProcessor));
            return processor;
        }
        return keyProcessor;
    }

    public ItemWriter<K> writer() {
        return new ProcessingItemWriter<>(keyValueProcessor(), keyValueWriter());
    }

    public KeyValueItemProcessor<K, V> keyValueProcessor() {
        KeyValueItemProcessor<K, V> keyValueReader = new KeyValueItemProcessor<>(client, codec);
        keyValueReader.setMemoryUsageLimit(memoryUsageLimit);
        keyValueReader.setMemoryUsageSamples(memoryUsageSamples);
        keyValueReader.setValueType(valueType);
        keyValueReader.setPoolSize(poolSize);
        keyValueReader.setReadFrom(readFrom);
        return keyValueReader;
    }

    private ItemWriter<KeyValue<K>> keyValueWriter() {
        queue = new LinkedBlockingQueue<>(queueCapacity);
        Helper.createGaugeCollectionSize("reader.queue.size", queue);
        QueueItemWriter<KeyValue<K>> writer = new QueueItemWriter<>(queue);
        if (shouldBlockKeys()) {
            CompositeItemWriter<KeyValue<K>> composite = new CompositeItemWriter<>();
            composite.setDelegates(Arrays.asList(writer, blockedKeyWriter));
            return composite;
        }
        return writer;
    }

    private boolean shouldBlockKeys() {
        return mode == Mode.LIVE && memoryUsageLimit != null;
    }

    @Override
    public synchronized void close() {
        if (isOpen()) {
            queue = null;
            if (jobExecution.isRunning()) {
                for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                    stepExecution.setTerminateOnly();
                }
                jobExecution.setStatus(BatchStatus.STOPPING);
            }
            jobExecution = null;
        }
        super.close();
    }

    public boolean isOpen() {
        return jobExecution != null;
    }

    @Override
    public synchronized KeyValue<K> read() throws Exception {
        KeyValue<K> item;
        do {
            item = queue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } while (item == null && jobExecution != null && jobExecution.isRunning());
        if (jobExecution != null && jobExecution.getStatus().isUnsuccessful()) {
            throw new ItemStreamException("Reader job failed");
        }
        return item;
    }

    @Override
    public KeyValue<K> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public synchronized List<KeyValue<K>> read(int maxElements) {
        List<KeyValue<K>> items = new ArrayList<>(maxElements);
        queue.drainTo(items, maxElements);
        return items;
    }

    private class BlockedKeyItemWriter extends AbstractItemStreamItemWriter<KeyValue<K>> {

        private final Set<String> blockedKeys = new HashSet<>();

        private final Function<K, String> toStringKeyFunction;

        private final Predicate<KeyValue<K>> predicate = v -> v.getMemoryUsage() > memoryUsageLimit.toBytes();

        public BlockedKeyItemWriter(RedisCodec<K, ?> codec) {
            this.toStringKeyFunction = Helper.toStringKeyFunction(codec);
        }

        @Override
        public void write(List<? extends KeyValue<K>> items) throws Exception {
            items.stream().filter(predicate).map(KeyValue::getKey).map(toStringKeyFunction).forEach(blockedKeys::add);
        }

        public Set<String> getBlockedKeys() {
            return blockedKeys;
        }

    }

}
