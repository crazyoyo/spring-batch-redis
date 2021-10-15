package com.redis.spring.batch.support;

import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;
import org.springframework.batch.core.step.item.ChunkProvider;
import org.springframework.batch.core.step.item.FaultTolerantChunkProvider;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;

public class FlushingStepBuilder<I, O> extends FaultTolerantStepBuilder<I, O> {

    public final static Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

    private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
    private Duration idleTimeout; // no idle stream detection by default

    public FlushingStepBuilder(StepBuilderHelper<?> parent) {
        super(parent);
    }

    public FlushingStepBuilder(SimpleStepBuilder<I, O> parent) {
        super(parent);
    }

    @Override
    protected ChunkProvider<I> createChunkProvider() {

        SkipPolicy readSkipPolicy = createSkipPolicy();
        readSkipPolicy = getFatalExceptionAwareProxy(readSkipPolicy);
        FlushingChunkProvider<I> chunkProvider = new FlushingChunkProvider<>(getReader(), createChunkOperations());
        chunkProvider.setMaxSkipsOnRead(Math.max(getChunkSize(), FaultTolerantChunkProvider.DEFAULT_MAX_SKIPS_ON_READ));
        chunkProvider.setSkipPolicy(readSkipPolicy);
        chunkProvider.setRollbackClassifier(getRollbackClassifier());
        chunkProvider.setFlushingInterval(flushingInterval);
        chunkProvider.setIdleTimeout(idleTimeout);
        ArrayList<StepListener> listeners = new ArrayList<>(getItemListeners());
        listeners.addAll(getSkipListeners());
        chunkProvider.setListeners(listeners);

        return chunkProvider;
    }

    @Override
    public FlushingStepBuilder<I, O> reader(ItemReader<? extends I> reader) {
        Assert.state(reader instanceof PollableItemReader, "Reader must be an instance of PollableItemReader");
        return (FlushingStepBuilder<I, O>) super.reader(reader);
    }

    public FlushingStepBuilder<I, O> flushingInterval(Duration flushingInterval) {
        Assert.notNull(flushingInterval, "Flushing interval must not be null");
        this.flushingInterval = flushingInterval;
        return this;
    }

    public FlushingStepBuilder<I, O> idleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }


}
