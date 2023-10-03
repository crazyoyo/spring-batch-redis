package com.redis.spring.batch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.lang.NonNull;
import org.springframework.retry.RetryPolicy;
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
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.writer.BlockingQueueItemWriter;

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

    public static final SkipPolicy DEFAULT_SKIP_POLICY = new NeverSkipItemSkipPolicy();

    public static final RetryPolicy DEFAULT_RETRY_POLICY = new MaxAttemptsRetryPolicy();

    private static final Duration DEFAULT_OPEN_TIMEOUT = Duration.ofSeconds(3);

    protected final AbstractRedisClient client;

    protected final RedisCodec<K, V> codec;

    private ReaderMode mode = DEFAULT_MODE;

    private SkipPolicy skipPolicy = DEFAULT_SKIP_POLICY;

    private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;

    private List<Class<? extends Throwable>> retriableExceptions = defaultRetriableExceptions();

    private List<Class<? extends Throwable>> notRetriableExceptions = defaultNotRetriableExceptions();

    private int database;

    private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

    private int notificationQueueCapacity = DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    private Duration flushInterval = DEFAULT_FLUSH_INTERVAL;

    private Duration idleTimeout;

    private long scanCount;

    private ItemProcessor<K, K> keyProcessor;

    protected ReadFrom readFrom;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private String keyPattern;

    private DataType keyType;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private Duration openTimeout = DEFAULT_OPEN_TIMEOUT;

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private String name;

    private JobExecution jobExecution;

    private KeyItemReader<K> keyReader;

    private ProcessingItemWriter<K, T> writer;

    private BlockingQueue<T> queue;

    private boolean open;

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

    public void addRetriableException(Class<? extends Throwable> exception) {
        retriableExceptions.add(exception);
    }

    public void setRetryPolicy(RetryPolicy policy) {
        this.retryPolicy = policy;
    }

    public void setSkipPolicy(SkipPolicy policy) {
        this.skipPolicy = policy;
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
        if (jobExecution == null) {
            initializeJobRepository();
            JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
            Job job = jobBuilderFactory.get(name).start(step().build()).build();
            SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(jobRepository);
            jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
            try {
                jobExecution = jobLauncher.run(job, new JobParameters());
            } catch (JobExecutionException e) {
                throw new ItemStreamException("Job execution failed", e);
            }
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleWithFixedDelay(() -> {
                if (jobExecution.isRunning() && keyReader.isOpen() || jobExecution.getStatus().isUnsuccessful()
                        || jobExecution.getStatus().equals(BatchStatus.COMPLETED)) {
                    executor.shutdown();
                }
            }, 0, pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
            try {
                boolean terminated = executor.awaitTermination(openTimeout.toMillis(), TimeUnit.MILLISECONDS);
                if (!terminated) {
                    throw new ItemStreamException("Timeout waiting for job to run");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ItemStreamException("Interruped while waiting for job to start", e);
            }
            if (jobExecution.getStatus().isUnsuccessful()) {
                if (CollectionUtils.isEmpty(jobExecution.getAllFailureExceptions())) {
                    throw new ItemStreamException("Could not run job");
                }
                throw new ItemStreamException("Could not run job", jobExecution.getAllFailureExceptions().get(0));
            }
            open = true;
        }
    }

    @Override
    public void close() {
        super.close();
        open = false;
    }

    @Override
    public synchronized T read() throws Exception {
        T item;
        do {
            item = poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } while (item == null && jobExecution.isRunning());
        return item;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    private @NonNull JobRepository initializeJobRepository() {
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
        return jobRepository;
    }

    private SimpleStepBuilder<K, K> step() {
        SimpleStepBuilder<K, K> step = new StepBuilder(name).chunk(chunkSize);
        step.repository(jobRepository);
        step.transactionManager(transactionManager);
        keyReader = reader();
        step.reader(keyReader);
        step.processor(keyProcessor);
        writer = writer();
        step.writer(writer);
        if (threads > 1) {
            step.taskExecutor(BatchUtils.threadPoolTaskExecutor(threads));
            step.throttleLimit(threads);
        }
        if (isLive()) {
            FlushingStepBuilder<K, K> flushingStep = new FlushingStepBuilder<>(step);
            flushingStep.interval(flushInterval);
            flushingStep.idleTimeout(idleTimeout);
            step = flushingStep;
        }
        FaultTolerantStepBuilder<K, K> ftStep = step.faultTolerant();
        ftStep.skipPolicy(skipPolicy);
        ftStep.retryPolicy(retryPolicy);
        retriableExceptions.forEach(ftStep::retry);
        notRetriableExceptions.forEach(ftStep::noRetry);
        return ftStep;
    }

    private ProcessingItemWriter<K, T> writer() {
        queue = new LinkedBlockingQueue<>(queueCapacity);
        Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
        return new ProcessingItemWriter<>(processor(), new BlockingQueueItemWriter<>(queue));
    }

    private static class ProcessingItemWriter<K, T> extends AbstractItemStreamItemWriter<K> {

        private final ItemProcessor<List<? extends K>, List<T>> processor;

        private final ItemWriter<T> writer;

        public ProcessingItemWriter(ItemProcessor<List<? extends K>, List<T>> processor, ItemWriter<T> writer) {
            this.processor = processor;
            this.writer = writer;
        }

        @Override
        public void open(ExecutionContext executionContext) {
            if (processor instanceof ItemStream) {
                ((ItemStream) processor).open(executionContext);
            }
            if (writer instanceof ItemStream) {
                ((ItemStream) writer).open(executionContext);
            }
            super.open(executionContext);
        }

        @Override
        public void update(ExecutionContext executionContext) {
            if (processor instanceof ItemStream) {
                ((ItemStream) processor).update(executionContext);
            }
            if (writer instanceof ItemStream) {
                ((ItemStream) writer).update(executionContext);
            }
            super.update(executionContext);
        }

        @Override
        public void close() {
            if (processor instanceof ItemStream) {
                ((ItemStream) processor).close();
            }
            if (writer instanceof ItemStream) {
                ((ItemStream) writer).close();
            }
            super.close();
        }

        @Override
        public void write(List<? extends K> items) throws Exception {
            List<T> values = processor.process(items);
            if (values != null) {
                writer.write(values);
            }
        }

    }

    protected abstract ItemProcessor<List<? extends K>, List<T>> processor();

    private KeyItemReader<K> reader() {
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

    private boolean isLive() {
        return mode == ReaderMode.LIVE;
    }

    public synchronized boolean isOpen() {
        return open;
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
    public static List<Class<? extends Throwable>> defaultNotRetriableExceptions() {
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
