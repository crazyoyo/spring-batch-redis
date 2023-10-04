package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.reader.DumpItemReader;
import com.redis.spring.batch.reader.KeyItemReader;
import com.redis.spring.batch.reader.KeyTypeItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader.OrderingStrategy;
import com.redis.spring.batch.reader.PollableItemReader;
import com.redis.spring.batch.reader.ScanKeyItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.step.FlushingChunkProvider;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.util.Await;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.micrometer.core.instrument.Metrics;

public abstract class RedisItemReader<K, V, T> extends AbstractItemStreamItemReader<T> implements PollableItemReader<T> {

    public enum ReaderMode {
        SCAN, LIVE
    }

    public static final String QUEUE_METER = "redis.batch.reader.queue.size";

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    public static final int DEFAULT_THREADS = 1;

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final Duration DEFAULT_FLUSH_INTERVAL = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(10);

    public static final OrderingStrategy DEFAULT_ORDERING = OrderingStrategy.PRIORITY;

    public static final int DEFAULT_NOTIFICATION_QUEUE_CAPACITY = 10000;

    public static final ReaderMode DEFAULT_MODE = ReaderMode.SCAN;

    public static final int DEFAULT_SKIP_LIMIT = 0;

    public static final int DEFAULT_RETRY_LIMIT = MaxAttemptsRetryPolicy.DEFAULT_MAX_ATTEMPTS;

    private static final Duration DEFAULT_OPEN_TIMEOUT = Duration.ofSeconds(3);

    private final AbstractRedisClient client;

    private final RedisCodec<K, V> codec;

    private ReaderMode mode = DEFAULT_MODE;

    private int skipLimit = DEFAULT_SKIP_LIMIT;

    private int retryLimit = DEFAULT_RETRY_LIMIT;

    private List<Class<? extends Throwable>> skippableExceptions = defaultNonRetriableExceptions();

    private List<Class<? extends Throwable>> nonSkippableExceptions = defaultRetriableExceptions();

    private List<Class<? extends Throwable>> retriableExceptions = defaultRetriableExceptions();

    private List<Class<? extends Throwable>> nonRetriableExceptions = defaultNonRetriableExceptions();

    private int database;

    private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

    private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout;

    private long scanCount;

    private ItemProcessor<K, K> keyProcessor;

    private ReadFrom readFrom;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private String keyPattern;

    private DataType keyType;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private Duration openTimeout = DEFAULT_OPEN_TIMEOUT;

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private SimpleJobLauncher jobLauncher;

    private String name;

    private JobExecution jobExecution;

    private KeyItemReader<K> keyReader;

    private BlockingQueue<T> queue;

    protected RedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        setName(ClassUtils.getShortName(getClass()));
        this.client = client;
        this.codec = codec;
    }

    public AbstractRedisClient getClient() {
        return client;
    }

    public RedisCodec<K, V> getCodec() {
        return codec;
    }

    public ReaderMode getMode() {
        return mode;
    }

    public void addSkippableException(Class<? extends Throwable> exception) {
        skippableExceptions.add(exception);
    }

    public void addNonSkippableException(Class<? extends Throwable> exception) {
        nonSkippableExceptions.add(exception);
    }

    public void addRetriableException(Class<? extends Throwable> exception) {
        retriableExceptions.add(exception);
    }

    public void addNonRetriableException(Class<? extends Throwable> exception) {
        nonRetriableExceptions.add(exception);
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }

    public void setScanCount(long count) {
        this.scanCount = count;
    }

    public void setOpenTimeout(Duration timeout) {
        this.openTimeout = timeout;
    }

    public void setJobRepository(JobRepository repository) {
        this.jobRepository = repository;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setKeyProcessor(ItemProcessor<K, K> processor) {
        this.keyProcessor = processor;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setChunkSize(int size) {
        this.chunkSize = size;
    }

    public void setQueueCapacity(int capacity) {
        this.queueCapacity = capacity;
    }

    public void setMode(ReaderMode mode) {
        this.mode = mode;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public void setKeyPattern(String globPattern) {
        this.keyPattern = globPattern;
    }

    public void setKeyType(DataType type) {
        this.keyType = type;
    }

    public void setPollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
    }

    public void setIdleTimeout(Duration timeout) {
        this.idleTimeout = timeout;
    }

    public List<Class<? extends Throwable>> getRetriableExceptions() {
        return retriableExceptions;
    }

    public void setRetriableExceptions(List<Class<? extends Throwable>> retriableExceptions) {
        this.retriableExceptions = retriableExceptions;
    }

    public List<Class<? extends Throwable>> getNonRetriableExceptions() {
        return nonRetriableExceptions;
    }

    public void setNonRetriableExceptions(List<Class<? extends Throwable>> nonRetriableExceptions) {
        this.nonRetriableExceptions = nonRetriableExceptions;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public int getDatabase() {
        return database;
    }

    public OrderingStrategy getOrderingStrategy() {
        return orderingStrategy;
    }

    public int getNotificationQueueCapacity() {
        return notificationQueueCapacity;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public long getScanCount() {
        return scanCount;
    }

    public ReadFrom getReadFrom() {
        return readFrom;
    }

    public int getThreads() {
        return threads;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public String getKeyPattern() {
        return keyPattern;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public Duration getOpenTimeout() {
        return openTimeout;
    }

    public void setFlushInterval(Duration interval) {
        this.flushInterval = interval;
    }

    public void setNotificationQueueCapacity(int capacity) {
        this.notificationQueueCapacity = capacity;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setOrderingStrategy(OrderingStrategy strategy) {
        this.orderingStrategy = strategy;
    }

    @Override
    public void setName(String name) {
        super.setName(name);
        this.name = name;
    }

    public ItemReader<K> getKeyReader() {
        return keyReader;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) {
        super.open(executionContext);
        if (!isOpen()) {
            initializeJobInfrastructure();
            Job job = jobBuilderFactory.get(name).start(step().build()).build();
            jobExecution = launch(job);
        }
    }

    private JobExecution launch(Job job) {
        JobExecution execution;
        try {
            execution = jobLauncher.run(job, new JobParameters());
        } catch (JobExecutionException e) {
            throw new ItemStreamException("Job execution failed", e);
        }
        Await await = new Await();
        boolean executed;
        try {
            executed = await.await(() -> isRunning(execution), openTimeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ItemStreamException("Interruped while waiting for job to start", e);
        }
        if (!executed) {
            throw new ItemStreamException("Timeout waiting for job to run");
        }
        if (execution.getStatus().isUnsuccessful()) {
            if (CollectionUtils.isEmpty(execution.getAllFailureExceptions())) {
                throw new ItemStreamException("Could not run job");
            }
            throw new ItemStreamException("Could not run job", execution.getAllFailureExceptions().get(0));
        }
        return execution;
    }

    private boolean isRunning(JobExecution execution) {
        return execution.isRunning() && keyReader.isOpen() || execution.getStatus().isUnsuccessful()
                || execution.getStatus().equals(BatchStatus.COMPLETED);
    }

    @Override
    public synchronized void close() {
        super.close();
        if (isOpen()) {
            jobExecution = null;
        }
    }

    @Override
    public synchronized T read() throws Exception {
        T item;
        do {
            item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } while (item == null && keyReader.isOpen());
        return item;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    private void initializeJobInfrastructure() {
        if (jobRepository == null || transactionManager == null) {
            @SuppressWarnings("deprecation")
            AbstractJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
            try {
                jobRepository = bean.getObject();
                transactionManager = bean.getTransactionManager();
            } catch (Exception e) {
                throw new ItemStreamException("Could not initialize job repository");
            }
        }
        jobBuilderFactory = new JobBuilderFactory(jobRepository);
        stepBuilderFactory = new StepBuilderFactory(jobRepository, transactionManager);
        jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
    }

    private SimpleStepBuilder<K, K> step() {
        FaultTolerantStepBuilder<K, K> step = simpleStep().faultTolerant();
        keyReader = keyReader();
        step.reader(keyReader);
        step.processor(keyProcessor);
        step.writer(writer());
        if (threads > 1) {
            step.taskExecutor(BatchUtils.threadPoolTaskExecutor(threads));
            step.throttleLimit(threads);
        }
        step.skipLimit(skipLimit);
        step.retryLimit(retryLimit);
        skippableExceptions.forEach(step::skip);
        nonSkippableExceptions.forEach(step::noSkip);
        retriableExceptions.forEach(step::retry);
        nonRetriableExceptions.forEach(step::noRetry);
        return step;
    }

    private SimpleStepBuilder<K, K> simpleStep() {
        SimpleStepBuilder<K, K> step = stepBuilderFactory.get(name).chunk(chunkSize);
        if (isLive()) {
            FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(step);
            flushingStep.interval(flushInterval);
            flushingStep.idleTimeout(idleTimeout);
            return flushingStep;
        }
        return step;
    }

    private ProcessingItemWriter writer() {
        queue = new LinkedBlockingQueue<>(queueCapacity);
        Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
        return new ProcessingItemWriter(processor());
    }

    private class ProcessingItemWriter extends AbstractItemStreamItemWriter<K> {

        private final ItemProcessor<List<? extends K>, List<T>> processor;

        public ProcessingItemWriter(ItemProcessor<List<? extends K>, List<T>> processor) {
            this.processor = processor;
        }

        @Override
        public void open(ExecutionContext executionContext) {
            if (processor instanceof ItemStream) {
                ((ItemStream) processor).open(executionContext);
            }
            super.open(executionContext);
        }

        @Override
        public void update(ExecutionContext executionContext) {
            if (processor instanceof ItemStream) {
                ((ItemStream) processor).update(executionContext);
            }
            super.update(executionContext);
        }

        @Override
        public void close() {
            if (processor instanceof ItemStream) {
                ((ItemStream) processor).close();
            }
            super.close();
        }

        @Override
        public void write(List<? extends K> items) throws Exception {
            List<T> values = processor.process(items);
            if (values != null) {
                for (T item : values) {
                    queue.put(item);
                }
            }
        }

    }

    protected abstract ItemProcessor<List<? extends K>, List<T>> processor();

    private KeyItemReader<K> keyReader() {
        if (isLive()) {
            return keyspaceNotificationReader();
        }
        return scanKeyReader();
    }

    private KeyspaceNotificationItemReader<K> keyspaceNotificationReader() {
        KeyspaceNotificationItemReader<K> reader = new KeyspaceNotificationItemReader<>(client, codec);
        reader.setDatabase(database);
        reader.setKeyPattern(keyPattern);
        reader.setKeyType(keyType);
        reader.setOrderingStrategy(orderingStrategy);
        reader.setQueueCapacity(notificationQueueCapacity);
        reader.setPollTimeout(pollTimeout);
        return reader;
    }

    public ScanKeyItemReader<K> scanKeyReader() {
        ScanKeyItemReader<K> reader = new ScanKeyItemReader<>(client, codec);
        reader.setReadFrom(readFrom);
        reader.setLimit(scanCount);
        reader.setMatch(keyPattern);
        reader.setType(keyType == null ? null : keyType.getString());
        return reader;
    }

    public boolean isLive() {
        return mode == ReaderMode.LIVE;
    }

    public synchronized boolean isOpen() {
        return jobExecution != null;
    }

    public static DumpItemReader dump(AbstractRedisClient client) {
        return new DumpItemReader(client);
    }

    public static StructItemReader<String, String> struct(AbstractRedisClient client) {
        return struct(client, StringCodec.UTF8);
    }

    public static <K, V> StructItemReader<K, V> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new StructItemReader<>(client, codec);
    }

    @SuppressWarnings("unchecked")
    public static List<Class<? extends Throwable>> defaultRetriableExceptions() {
        return modifiableList(RedisCommandTimeoutException.class);
    }

    @SuppressWarnings("unchecked")
    public static List<Class<? extends Throwable>> defaultNonRetriableExceptions() {
        return modifiableList(RedisCommandExecutionException.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> modifiableList(T... elements) {
        return new ArrayList<>(Arrays.asList(elements));
    }

    public static KeyTypeItemReader<String, String> type(AbstractRedisClient client) {
        return type(client, StringCodec.UTF8);
    }

    public static <K, V> KeyTypeItemReader<K, V> type(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new KeyTypeItemReader<>(client, codec);
    }

}
