package org.springframework.batch.item.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.*;
import org.springframework.batch.item.redis.support.*;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KeyValueItemReader<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemReader<T> {

    private final ItemReader<K> keyReader;
    private final ItemProcessor<List<? extends K>, List<T>> valueReader;
    private final int threads;
    private final int chunkSize;
    private final int queueCapacity;
    private final long queuePollTimeout;
    protected BlockingQueue<T> queue;
    private JobExecution jobExecution;
    private String name;

    public KeyValueItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader, int chunkSize, int threads, int queueCapacity, Duration queuePollTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(keyReader, "A key reader is required");
        Assert.notNull(valueReader, "A value reader is required");
        Assert.isTrue(chunkSize > 0, "Chunk size must be greater than zero");
        Assert.isTrue(threads > 0, "Thread count must be greater than zero");
        Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero");
        Assert.notNull(queuePollTimeout, "Queue poll timeout must not be null");
        Assert.isTrue(!queuePollTimeout.isZero(), "Queue poll timeout must not be zero");
        Assert.isTrue(!queuePollTimeout.isNegative(), "Queue poll timeout must not be negative");
        this.keyReader = keyReader;
        this.valueReader = valueReader;
        this.threads = threads;
        this.chunkSize = chunkSize;
        this.queueCapacity = queueCapacity;
        this.queuePollTimeout = queuePollTimeout.toMillis();
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
        MetricsUtils.createGaugeCollectionSize("reader.queue.size", queue);
        ItemWriter<K> writer = new ValueWriter<>(valueReader, queue);
        JobFactory factory = new JobFactory();
        try {
            factory.afterPropertiesSet();
        } catch (Exception e) {
            throw new ItemStreamException("Failed to initialize the reader", e);
        }
        StepBuilder stepBuilder = factory.getStepBuilderFactory().get(name + "-step");
        SimpleStepBuilder<K, K> simpleStepBuilder = simpleStepBuilder(stepBuilder);
        simpleStepBuilder.reader(keyReader);
        simpleStepBuilder.writer(writer);
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(threads);
        taskExecutor.setCorePoolSize(threads);
        taskExecutor.afterPropertiesSet();
        TaskletStep step = simpleStepBuilder.taskExecutor(taskExecutor).throttleLimit(threads).build();
        Job job = factory.getJobBuilderFactory().get(name + "-job").start(step).build();
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

    protected SimpleStepBuilder<K, K> simpleStepBuilder(StepBuilder stepBuilder) {
        return stepBuilder.chunk(chunkSize);
    }

    @Override
    public T read() throws Exception {
        T item;
        do {
            item = queue.poll(queuePollTimeout, TimeUnit.MILLISECONDS);
        } while (item == null && jobExecution.isRunning());
        return item;
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

    private static class ValueWriter<K, T extends KeyValue<K, ?>> extends AbstractItemStreamItemWriter<K> {

        private final ItemProcessor<List<? extends K>, List<T>> valueReader;
        private final BlockingQueue<T> queue;

        private ValueWriter(ItemProcessor<List<? extends K>, List<T>> valueReader, BlockingQueue<T> queue) {
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
        public void write(List<? extends K> items) throws Exception {
            for (T value : valueReader.process(items)) {
                queue.removeIf(v -> v.getKey().equals(value.getKey()));
                queue.put(value);
            }
        }

    }

    public static <T extends KeyValue<String, ?>> IntermediaryKeyValueItemReaderBuilder<T> valueReader(ItemProcessor<List<? extends String>, List<T>> valueReader) {
        return new IntermediaryKeyValueItemReaderBuilder<>(valueReader);
    }

    public static class IntermediaryKeyValueItemReaderBuilder<T extends KeyValue<String, ?>> {

        private final ItemProcessor<List<? extends String>, List<T>> valueReader;

        public IntermediaryKeyValueItemReaderBuilder(ItemProcessor<List<? extends String>, List<T>> valueReader) {
            this.valueReader = valueReader;
        }

        public KeyValueItemReaderBuilder<T> keyReader(ItemReader<String> keyReader) {
            return new KeyValueItemReaderBuilder<>(keyReader, valueReader);
        }

        public KeyspaceNotificationValueItemReader.KeyspaceNotificationValueItemReaderBuilder<T> keyReader(PollableItemReader<String> keyReader) {
            return new KeyspaceNotificationValueItemReader.KeyspaceNotificationValueItemReaderBuilder<>(keyReader, valueReader);
        }

    }

    @SuppressWarnings({"unchecked", "unused"})
    public static class AbstractKeyValueItemReaderBuilder<T extends KeyValue<String, ?>, B extends AbstractKeyValueItemReaderBuilder<T, B>> {

        public static final int DEFAULT_CHUNK_SIZE = 50;
        public static final int DEFAULT_THREAD_COUNT = 1;
        public static final int DEFAULT_QUEUE_CAPACITY = 1000;
        public static final Duration DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);

        protected final ItemReader<String> keyReader;
        protected final ItemProcessor<List<? extends String>, List<T>> valueReader;
        protected int chunkSize = DEFAULT_CHUNK_SIZE;
        protected int threadCount = DEFAULT_THREAD_COUNT;
        protected int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        protected Duration queuePollTimeout = DEFAULT_QUEUE_POLL_TIMEOUT;

        protected AbstractKeyValueItemReaderBuilder(ItemReader<String> keyReader, ItemProcessor<List<? extends String>, List<T>> valueReader) {
            this.keyReader = keyReader;
            this.valueReader = valueReader;
        }

        public B chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return (B) this;
        }

        public B threadCount(int threadCount) {
            this.threadCount = threadCount;
            return (B) this;
        }

        public B queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return (B) this;
        }

        public B queuePollTimeout(Duration queuePollTimeout) {
            this.queuePollTimeout = queuePollTimeout;
            return (B) this;
        }

    }

    public static class KeyValueItemReaderBuilder<T extends KeyValue<String, ?>> extends AbstractKeyValueItemReaderBuilder<T, KeyValueItemReaderBuilder<T>> {

        public KeyValueItemReaderBuilder(ItemReader<String> keyReader, ItemProcessor<List<? extends String>, List<T>> valueReader) {
            super(keyReader, valueReader);
        }

        public KeyValueItemReader<String, T> build() {
            return new KeyValueItemReader<>(keyReader, valueReader, chunkSize, threadCount, queueCapacity, queuePollTimeout);
        }
    }


}
