package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public abstract class AbstractKeyValueItemReader<K, V, C extends StatefulConnection<K, V>, T extends KeyValue<K, ?>> extends AbstractPollableItemReader<T> {

    @Getter
    private final ItemReader<K> keyReader;
    protected final GenericObjectPool<C> pool;
    protected final Function<C, BaseRedisAsyncCommands<K, V>> commands;
    private final int threads;
    private final int chunkSize;
    private final BlockingQueue<T> queue;
    private final Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider;
    private JobExecution jobExecution;
    private String name;

    protected AbstractKeyValueItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, int chunkSize, int threads, int queueCapacity) {
        this(readTimeout, keyReader, pool, commands, chunkSize, threads, queueCapacity, Function.identity());
    }

    protected AbstractKeyValueItemReader(Duration readTimeout, ItemReader<K> keyReader, GenericObjectPool<C> pool, Function<C, BaseRedisAsyncCommands<K, V>> commands, int chunkSize, int threads, int queueCapacity, Function<SimpleStepBuilder<K, K>, SimpleStepBuilder<K, K>> stepBuilderProvider) {
        super(readTimeout);
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required.");
        Assert.notNull(pool, "A connection pool is required");
        Assert.notNull(commands, "A command function is required");
        Assert.isTrue(chunkSize > 0, "Chunk size must be greater than zero.");
        Assert.isTrue(threads > 0, "Thread count must be greater than zero.");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero.");
        this.keyReader = keyReader;
        this.pool = pool;
        this.commands = commands;
        this.threads = threads;
        this.chunkSize = chunkSize;
        this.queue = new LinkedBlockingDeque<>(queueCapacity);
        this.stepBuilderProvider = stepBuilderProvider;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void setName(String name) {
        this.name = name;
        super.setName(name);
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    protected boolean isRunning() {
        return super.isRunning() && jobExecution.isRunning();
    }

    @SuppressWarnings("BusyWait")
    @Override
    protected void doOpen() throws Exception {
        log.debug("Opening {}", name);
        JobFactory factory = new JobFactory();
        factory.afterPropertiesSet();
        SimpleStepBuilder<K, K> stepBuilder = stepBuilderProvider.apply(factory.step(name + "-step").chunk(chunkSize)).reader(keyReader).writer(this::addToQueue);
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(threads);
        TaskletStep step = stepBuilder.taskExecutor(taskExecutor).throttleLimit(threads).build();
        Job job = factory.job(name + "-job").start(step).build();
        MetricsUtils.createGaugeCollectionSize("reader.queue.size", queue);
        this.jobExecution = factory.getAsyncLauncher().run(job, new JobParameters());
        while (!jobExecution.isRunning()) {
            Thread.sleep(10);
        }
        super.doOpen();
        log.debug("Opened {}", name);
    }

    @SuppressWarnings("BusyWait")
    @Override
    protected void doClose() throws Exception {
        log.debug("Closing {}", name);
        super.doClose();
        if (!queue.isEmpty()) {
            log.warn("Closing {} with {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
        }
        while (jobExecution.isRunning()) {
            Thread.sleep(10);
        }
        jobExecution = null;
        log.debug("Closed {}", name);
    }

    public abstract List<T> values(List<? extends K> keys) throws Exception;

    private void addToQueue(List<? extends K> keys) throws Exception {
        for (T value : values(keys)) {
            queue.removeIf(v -> v.getKey().equals(value.getKey()));
            queue.put(value);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static class KeyValueItemReaderBuilder<B extends KeyValueItemReaderBuilder<B>> extends PollableItemReaderBuilder<B> {

        public static final int DEFAULT_QUEUE_CAPACITY = 1000;
        public static final int DEFAULT_CHUNK_SIZE = 50;
        public static final int DEFAULT_THREAD_COUNT = 1;

        protected int chunkSize = DEFAULT_CHUNK_SIZE;
        protected int threadCount = DEFAULT_THREAD_COUNT;
        protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;

        public B chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return (B) this;
        }

        @SuppressWarnings("unused")
        public B threadCount(int threadCount) {
            this.threadCount = threadCount;
            return (B) this;
        }

        public B queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return (B) this;
        }

    }

}
