package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.BatchOperationFunction;
import com.redis.spring.batch.common.BatchOperationFunction.BatchOperation;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.ProcessingItemWriter;
import com.redis.spring.batch.common.SimpleBatchOperation;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.ConnectionUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Metrics;

public abstract class AbstractRedisItemReader<K, V, T extends KeyValue<K, ?>> extends AbstractItemStreamItemReader<T> {

    private static final String LUA_FILENAME = "keyvalue.lua";

    public static final String QUEUE_METER = "redis.batch.reader.queue.size";

    public static final int DEFAULT_MEMORY_USAGE_SAMPLES = 5;

    public static final int DEFAULT_QUEUE_CAPACITY = 10000;

    public static final int DEFAULT_THREADS = 1;

    public static final int DEFAULT_CHUNK_SIZE = 50;

    public static final String MATCH_ALL = "*";

    public static final int DEFAULT_POOL_SIZE = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    /**
     * Default to no memory usage calculation
     */
    public static final DataSize DEFAULT_MEMORY_USAGE_LIMIT = DataSize.ofBytes(0);

    protected final AbstractRedisClient client;

    protected final RedisCodec<K, V> codec;

    private final LuaToKeyValueFunction<T> luaFunction;

    private ItemProcessor<K, K> keyProcessor;

    protected ReadFrom readFrom;

    private int threads = DEFAULT_THREADS;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private int poolSize = DEFAULT_POOL_SIZE;

    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

    private DataSize memoryUsageLimit = DEFAULT_MEMORY_USAGE_LIMIT;

    private int memoryUsageSamples = DEFAULT_MEMORY_USAGE_SAMPLES;

    protected String keyPattern;

    protected String keyType;

    protected Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    private String name;

    private JobExecution jobExecution;

    protected BlockingQueue<T> queue;

    private GenericObjectPool<StatefulConnection<K, V>> pool;

    protected AbstractRedisItemReader(AbstractRedisClient client, RedisCodec<K, V> codec, LuaToKeyValueFunction<T> lua) {
        setName(ClassUtils.getShortName(getClass()));
        this.client = client;
        this.codec = codec;
        this.luaFunction = lua;
    }

    public void setJobRepository(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
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

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public void setMemoryUsageLimit(DataSize memoryUsageLimit) {
        this.memoryUsageLimit = memoryUsageLimit;
    }

    public void setMemoryUsageSamples(int memoryUsageSamples) {
        this.memoryUsageSamples = memoryUsageSamples;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    public void setKeyPattern(String globPattern) {
        this.keyPattern = globPattern;
    }

    public void setKeyType(String type) {
        this.keyType = type;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
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
        if (jobRepository == null) {
            @SuppressWarnings("deprecation")
            AbstractJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
            try {
                bean.afterPropertiesSet();
                jobRepository = bean.getObject();
                transactionManager = bean.getTransactionManager();
            } catch (Exception e) {
                throw new ItemStreamException("Could not initialize job repository");
            }
        }
        queue = new LinkedBlockingQueue<>(queueCapacity);
        pool = pool();
        ProcessingItemWriter<K, T> writer = new ProcessingItemWriter<>(toKeyValueFunction(pool), valueWriter(queue));
        ItemReader<K> keyReader = keyReader();
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
    }

    private ItemWriter<T> valueWriter(BlockingQueue<T> queue) {
        return new QueueItemWriter<>(queue);
    }

    private static class QueueItemWriter<T> extends AbstractItemStreamItemWriter<T> {

        private final BlockingQueue<T> queue;

        public QueueItemWriter(BlockingQueue<T> queue) {
            this.queue = queue;
        }

        @Override
        public void open(ExecutionContext executionContext) {
            Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
            super.open(executionContext);
        }

        @Override
        public void write(List<? extends T> items) throws Exception {
            for (T item : items) {
                queue.put(item);
            }
        }

    }

    public GenericObjectPool<StatefulConnection<K, V>> pool() {
        Supplier<StatefulConnection<K, V>> connectionSupplier = ConnectionUtils.supplier(client, codec, readFrom);
        GenericObjectPoolConfig<StatefulConnection<K, V>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(poolSize);
        return ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, config);
    }

    public Function<List<? extends K>, List<T>> toKeyValueFunction(GenericObjectPool<StatefulConnection<K, V>> pool) {
        String digest = ConnectionUtils.loadScript(client, LUA_FILENAME);
        String type = luaFunction.getValueType();
        Evalsha<K, V> evalsha = new Evalsha<>(codec, digest, memoryUsageLimit.toBytes(), memoryUsageSamples, type);
        BatchOperation<K, V, K, List<Object>> operation = new SimpleBatchOperation<>(evalsha);
        BatchOperationFunction<K, V, K, List<Object>> operationFunction = new BatchOperationFunction<>(pool, operation);
        return operationFunction.andThen(new ToListFunction<>(luaFunction));
    }

    protected SimpleStepBuilder<K, K> step(ItemReader<K> reader, ItemWriter<K> writer) {
        SimpleStepBuilder<K, K> step = new StepBuilder(name).chunk(chunkSize);
        step.repository(jobRepository);
        step.transactionManager(transactionManager);
        step.reader(reader);
        step.processor(keyProcessor);
        step.writer(writer);
        if (threads > 1) {
            step.taskExecutor(BatchUtils.threadPoolTaskExecutor(threads));
            step.throttleLimit(threads);
        }
        return step;
    }

    protected abstract ItemReader<K> keyReader();

    @Override
    public synchronized void close() {
        super.close();
        if (!isOpen()) {
            return;
        }
        queue = null;
        pool.close();
        jobExecution = null;
    }

    public boolean isOpen() {
        return jobExecution != null;
    }

    @Override
    public synchronized T read() throws Exception {
        T item;
        do {
            item = queue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } while (item == null && isOpen() && jobExecution.isRunning());
        return item;
    }

    public synchronized List<T> readChunk() throws Exception {
        List<T> items = new ArrayList<>();
        T item;
        while (items.size() < chunkSize && (item = read()) != null) {
            items.add(item);
        }
        return items;
    }

}
