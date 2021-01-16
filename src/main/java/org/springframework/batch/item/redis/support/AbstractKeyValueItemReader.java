package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public abstract class AbstractKeyValueItemReader<K, V, T extends KeyValue<K, ?>, C extends StatefulConnection<K, V>> extends AbstractPollableItemReader<T> implements ValueReader<K, T>, BoundedItemReader<T> {

    private final int threads;
    private final int chunkSize;
    private final long commandTimeout;
    @Getter
    private final ItemReader<K> keyReader;
    private final ValueReader valueReader;
    private JobExecution jobExecution;
    private String name;

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
    public void setName(String name) {
        this.name = name;
        super.setName(name);
    }

    @Override
    protected void doOpen() throws Exception {
        JobFactory factory = new JobFactory();
        factory.afterPropertiesSet();
        SimpleStepBuilder<K, K> stepBuilder = factory.step(name + "-step", chunkSize, keyReader, null, valueReader);
        Job job = factory.job(name + "-job").start(factory.threads(stepBuilder, threads).build()).build();
        this.jobExecution = factory.getAsyncLauncher().run(job, new JobParameters());
        while (!jobExecution.isRunning()) {
            Thread.sleep(1);
        }
        if (keyReader instanceof AbstractKeyItemReader) {
            while (!((AbstractKeyItemReader<K, V, C>) keyReader).isOpen()) {
                Thread.sleep(1);
            }
        }
    }

    @Override
    public Long size() {
        if (keyReader instanceof BoundedItemReader) {
            return ((BoundedItemReader<K>) keyReader).size();
        }
        return null;
    }

    public boolean isRunning() {
        return jobExecution != null && jobExecution.isRunning();
    }

    @Override
    public boolean isTerminated() {
        return !isRunning();
    }

    @Override
    protected void doClose() throws InterruptedException {
        if (jobExecution != null) {
            jobExecution.stop();
        }
        while (!isTerminated()) {
            Thread.sleep(10);
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

}
