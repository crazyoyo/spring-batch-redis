package org.springframework.batch.item.redis.support;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.KeyValueItemReader;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KeyEventValueItemReader<K, T extends KeyValue<K, ?>> extends KeyValueItemReader<K, T> implements PollableItemReader<T> {

    private final Duration flushingInterval;
    private final Duration idleTimeout;

    public KeyEventValueItemReader(PollableItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout, Duration flushingInterval, Duration idleTimeout) {
        super(keyReader, valueReader, threads, chunkSize, queueCapacity, queuePollTimeout);
        Assert.notNull(flushingInterval, "Flushing interval must not be null");
        Assert.isTrue(!flushingInterval.isZero(), "Flushing interval must not be zero");
        Assert.isTrue(!flushingInterval.isNegative(), "Flushing interval must not be negative");
        this.flushingInterval = flushingInterval;
        this.idleTimeout = idleTimeout;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    protected SimpleStepBuilder<K, K> simpleStepBuilder(StepBuilder stepBuilder) {
        SimpleStepBuilder<K, K> simpleStepBuilder = super.simpleStepBuilder(stepBuilder);
        return new FlushingStepBuilder<>(simpleStepBuilder).flushingInterval(flushingInterval).idleTimeout(idleTimeout);
    }


}
