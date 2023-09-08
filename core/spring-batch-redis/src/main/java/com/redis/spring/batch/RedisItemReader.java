package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.reader.BlockedKeyItemWriter;
import com.redis.spring.batch.reader.KeyValueItemProcessor;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.util.ConnectionUtils;
import com.redis.spring.batch.util.PredicateItemProcessor;
import com.redis.spring.batch.util.Predicates;
import com.redis.spring.batch.writer.ProcessingItemWriter;
import com.redis.spring.batch.writer.QueueItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.Metrics;

public class RedisItemReader<K, V> extends AbstractItemStreamItemReader<KeyValue<K>>
        implements PollableItemReader<KeyValue<K>> {

    public enum Mode {
        SCAN, LIVE
    }

    public static final String QUEUE_METER = "redis.batch.reader.queue.size";

    public static final int DEFAULT_MEMORY_USAGE_SAMPLES = 5;

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    public static final ValueType DEFAULT_VALUE_TYPE = ValueType.DUMP;

    public static final int DEFAULT_THREADS = 1;

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final int DEFAULT_SCAN_COUNT = DEFAULT_CHUNK_SIZE;

    public static final String MATCH_ALL = "*";

    public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

    public static final int DEFAULT_DATABASE = 0;

    public static final String DEFAULT_PUBSUB_PATTERN = pattern(DEFAULT_DATABASE, MATCH_ALL);

    public static final OrderingStrategy DEFAULT_ORDERING = OrderingStrategy.PRIORITY;

    public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    public static final Duration DEFAULT_FLUSHING_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSHING_INTERVAL;

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private final Set<String> blockedKeys = new HashSet<>();

    protected ReadFrom readFrom;

    private ValueType valueType = DEFAULT_VALUE_TYPE;

    private Mode mode = Mode.SCAN;

    private ItemProcessor<K, K> keyProcessor;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private int poolSize = DEFAULT_POOL_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private DataSize memoryUsageLimit;

    private int memoryUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

    private String scanMatch;

    private String scanType;

    private long scanCount = DEFAULT_SCAN_COUNT;

    private int database = DEFAULT_DATABASE;

    private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

    private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;

    private Duration idleTimeout;

    private JobRepository jobRepository;

    private String name;

    private JobExecution jobExecution;

    private BlockingQueue<KeyValue<K>> queue;

    private ItemReader<K> keyReader;

    public RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        setName(ClassUtils.getShortName(getClass()));
        this.client = client;
        this.codec = codec;
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

    public ItemReader<K> getKeyReader() {
        return keyReader;
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

    public void setScanCount(long count) {
        this.scanCount = count;
    }

    public long getScanCount() {
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
        if (isOpen()) {
            return;
        }
        try {
            checkJobRepository();
        } catch (Exception e) {
            throw new ItemStreamException("Could not initialize job repository");
        }
        keyReader = keyReader();
        queue = new LinkedBlockingQueue<>(queueCapacity);
        Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
        QueueItemWriter<KeyValue<K>> queueWriter = new QueueItemWriter<>(queue);
        ItemWriter<KeyValue<K>> keyValueWriter = valueWriter(queueWriter);
        ProcessingItemWriter<K, KeyValue<K>> writer = new ProcessingItemWriter<>(keyValueProcessor(), keyValueWriter);
        SimpleStepBuilder<K, K> step = step(keyReader, writer);
        Job job = new JobBuilderFactory(jobRepository).get(name).start(step.build()).build();
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        try {
            jobExecution = jobLauncher.run(job, new JobParameters());
        } catch (JobExecutionException e) {
            throw new ItemStreamException("Job execution failed", e);
        }
        while (!(BatchUtils.isOpen(keyReader) || jobExecution.getStatus().isUnsuccessful()
                || jobExecution.getStatus().isLessThanOrEqualTo(BatchStatus.COMPLETED))) {
            sleep();
        }
        if (jobExecution.getStatus().isUnsuccessful()) {
            throw new ItemStreamException("Could not run job", jobExecution.getAllFailureExceptions().iterator().next());
        }
    }

    private ItemReader<K> keyReader() {
        if (isLive()) {
            return keyspaceNotificationReader();
        }
        return scanKeyReader();
    }

    @SuppressWarnings("unchecked")
    private ItemWriter<KeyValue<K>> valueWriter(ItemWriter<KeyValue<K>> writer) {
        if (blockKeys()) {
            BlockedKeyItemWriter<K> blockedKeyWriter = new BlockedKeyItemWriter<>(codec, memoryUsageLimit, blockedKeys);
            return BatchUtils.writer(writer, blockedKeyWriter);
        }
        return writer;
    }

    private boolean isLive() {
        return mode == Mode.LIVE;
    }

    private SimpleStepBuilder<K, K> step(ItemReader<K> reader, ItemWriter<K> writer) {
        SimpleStepBuilder<K, K> step = new StepBuilder(name).chunk(chunkSize);
        step.repository(jobRepository);
        step.transactionManager(transactionManager());
        step.reader(reader);
        step.processor(processor());
        step.writer(writer);
        if (threads > 1) {
            step.taskExecutor(BatchUtils.threadPoolTaskExecutor(threads));
            step.throttleLimit(threads);
        }
        if (isLive()) {
            FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(step);
            flushingStep.interval(flushingInterval);
            flushingStep.idleTimeout(idleTimeout);
            return flushingStep;
        }
        return step;
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

    private static String pattern(int database, String match) {
        return String.format(PUBSUB_PATTERN_FORMAT, database, match == null ? MATCH_ALL : match);
    }

    private ItemReader<K> scanKeyReader() {
        Supplier<StatefulConnection<K, V>> supplier = ConnectionUtils.supplier(client, codec, readFrom);
        StatefulConnection<K, V> connection = supplier.get();
        ScanIterator<K> iterator = ScanIterator.scan(ConnectionUtils.sync(connection), scanArgs());
        return new IteratorItemReader<>(iterator);
    }

    private KeyScanArgs scanArgs() {
        KeyScanArgs args = new KeyScanArgs();
        args.limit(scanCount);
        if (scanMatch != null) {
            args.match(scanMatch);
        }
        if (scanType != null) {
            args.type(scanType);
        }
        return args;
    }

    public Set<String> getBlockedKeys() {
        return blockedKeys;
    }

    @SuppressWarnings("deprecation")
    private void checkJobRepository() throws Exception {
        if (jobRepository == null) {
            org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
            bean.afterPropertiesSet();
            jobRepository = bean.getObject();
        }
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

    private ItemProcessor<K, K> processor() {
        if (blockKeys()) {
            Function<K, String> toStringKey = CodecUtils.toStringKeyFunction(codec);
            Predicate<K> predicate = Predicates.map(toStringKey, Predicates.negate(blockedKeys::contains));
            ItemProcessor<K, K> keyFilter = new PredicateItemProcessor<>(predicate);
            if (keyProcessor == null) {
                return keyFilter;
            }
            return BatchUtils.processor(keyFilter, keyProcessor);
        }
        return keyProcessor;
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

    private boolean blockKeys() {
        return memoryUsageLimit != null;
    }

    @Override
    public synchronized void close() {
        super.close();
        if (!isOpen()) {
            return;
        }
        queue = null;
        if (jobExecution.isRunning()) {
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                stepExecution.setTerminateOnly();
            }
            jobExecution.setStatus(BatchStatus.STOPPING);
        }
        jobExecution = null;
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

    public synchronized List<KeyValue<K>> readChunk() throws Exception {
        List<KeyValue<K>> items = new ArrayList<>();
        KeyValue<K> item;
        while (items.size() < chunkSize && (item = read()) != null) {
            items.add(item);
        }
        return items;
    }

}
