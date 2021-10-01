package org.springframework.batch.item.redis.support;

import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LiveKeyValueItemReader<T extends KeyValue<?>> extends KeyValueItemReader<T> implements PollableItemReader<T> {

    private final Duration flushingInterval;
    private final Duration idleTimeout;
    private State state;

    public LiveKeyValueItemReader(PollableItemReader<String> keyReader, ItemProcessor<List<? extends String>, List<T>> valueReader, int threads, int chunkSize, int queueCapacity, Duration queuePollTimeout, SkipPolicy skipPolicy, Duration flushingInterval, Duration idleTimeout) {
        super(keyReader, valueReader, threads, chunkSize, queueCapacity, queuePollTimeout, skipPolicy);
        Assert.notNull(flushingInterval, "Flushing interval must not be null");
        Assert.isTrue(!flushingInterval.isZero(), "Flushing interval must not be zero");
        Assert.isTrue(!flushingInterval.isNegative(), "Flushing interval must not be negative");
        this.flushingInterval = flushingInterval;
        this.idleTimeout = idleTimeout;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        super.open(executionContext);
        this.state = State.OPEN;
    }

    @Override
    public synchronized void close() {
        super.close();
        this.state = State.CLOSED;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    protected FaultTolerantStepBuilder<String, String> faultTolerantStepBuilder(SimpleStepBuilder<String, String> stepBuilder) {
        return new FlushingStepBuilder<>(stepBuilder).flushingInterval(flushingInterval).idleTimeout(idleTimeout);
    }

}
