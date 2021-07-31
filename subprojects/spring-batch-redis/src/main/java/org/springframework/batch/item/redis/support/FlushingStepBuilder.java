package org.springframework.batch.item.redis.support;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.item.ChunkOrientedTasklet;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;

public class FlushingStepBuilder<I, O> extends SimpleStepBuilder<I, O> {

    public final static Duration DEFAULT_FLUSHING_INTERVAL = Duration.ofMillis(50);

    private Duration flushingInterval = DEFAULT_FLUSHING_INTERVAL;
    private Duration idleTimeout; // no idle stream detection by default

    public FlushingStepBuilder(SimpleStepBuilder<I, O> parent) {
        super(parent);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Tasklet createTasklet() {
        Assert.notNull(getReader(), "ItemReader must be provided");
        Assert.state(getReader() instanceof PollableItemReader, "Reader must be an instance of PollableItemReader");
        Assert.notNull(getWriter(), "ItemWriter must be provided");
        FlushingChunkProvider<I> chunkProvider = new FlushingChunkProvider<>((PollableItemReader<I>) getReader(), createChunkOperations(), flushingInterval, idleTimeout);
        chunkProvider.setListeners(new ArrayList<>(getItemListeners()));
        SimpleChunkProcessor<I, O> chunkProcessor = new SimpleChunkProcessor<>(getProcessor(), getWriter());
        chunkProcessor.setListeners(new ArrayList<>(getItemListeners()));
        ChunkOrientedTasklet<I> tasklet = new ChunkOrientedTasklet<>(chunkProvider, chunkProcessor);
        tasklet.setBuffering(!isReaderTransactionalQueue());
        return tasklet;
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
