package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public abstract class AbstractKeyValueItemReader<K, V, T extends KeyValue<K, ?>, C extends StatefulConnection<K, V>> extends AbstractPollableItemReader<T> implements ValueReader<K, T> {

    private final ItemReader<K> keyReader;
    private final long commandTimeout;
    private final JobOptions jobOptions;
    private final int queueCapacity;
    private BlockingQueue<T> queue;
    private JobExecution jobExecution;

    protected AbstractKeyValueItemReader(ItemReader<K> keyReader, Duration commandTimeout, JobOptions jobOptions, int queueCapacity, Duration pollingTimeout) {
        super(pollingTimeout);
        Assert.notNull(keyReader, "Key reader is required.");
        Assert.notNull(jobOptions, "Job options are required.");
        this.jobOptions = jobOptions;
        this.queueCapacity = queueCapacity;
        this.keyReader = keyReader;
        this.commandTimeout = commandTimeout.getSeconds();
    }

    @Override
    protected void doOpen() throws Exception {
        queue = new LinkedBlockingDeque<>(queueCapacity);
        MetricsUtils.createGaugeCollectionSize("reader.queue.size", queue);
        JobFactory<K, K> factory = new JobFactory<>();
        factory.afterPropertiesSet();
        String name = ClassUtils.getShortName(getClass());
        TaskletStep step = factory.<K, K>step(name + "-step", jobOptions).reader(keyReader).writer(this::writeKeys).build();
        Job job = factory.getJobBuilderFactory().get(name + "-job").start(step).build();
        this.jobExecution = factory.executeAsync(job, new JobParameters());
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
        if (!queue.isEmpty()) {
            log.warn("Closing {} - {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
        }
        queue.clear();
    }

    private void writeKeys(List<? extends K> keys) throws Exception {
        for (T value : values(keys)) {
            queue.removeIf(v -> v.getKey().equals(value.getKey()));
            queue.put(value);
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
    public T poll(Duration timeout) throws InterruptedException {
        return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

}
