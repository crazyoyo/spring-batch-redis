package com.redis.spring.batch.step;

import java.util.ArrayList;
import java.util.function.Function;

import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilderHelper;
import org.springframework.batch.core.step.item.ChunkProvider;
import org.springframework.batch.core.step.item.FaultTolerantChunkProvider;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.util.Assert;

import com.redis.spring.batch.reader.PollableItemReader;

public class FlushingFaultTolerantStepBuilder<I, O> extends FaultTolerantStepBuilder<I, O> {

    private FlushingStepOptions options = FlushingStepOptions.builder().build();

    public FlushingFaultTolerantStepBuilder(StepBuilderHelper<?> parent) {
        super(parent);
    }

    public FlushingFaultTolerantStepBuilder(SimpleStepBuilder<I, O> parent) {
        super(parent);
    }

    public FlushingFaultTolerantStepBuilder(FlushingStepBuilder<I, O> parent) {
        super(parent);
        this.options = parent.getOptions();
    }

    @Override
    protected ChunkProvider<I> createChunkProvider() {
        SkipPolicy readSkipPolicy = createSkipPolicy();
        readSkipPolicy = getFatalExceptionAwareProxy(readSkipPolicy);
        int maxSkipsOnRead = Math.max(getChunkSize(), FaultTolerantChunkProvider.DEFAULT_MAX_SKIPS_ON_READ);
        FlushingFaultTolerantChunkProvider<I> chunkProvider = new FlushingFaultTolerantChunkProvider<>(getReader(),
                createChunkOperations());
        chunkProvider.setMaxSkipsOnRead(maxSkipsOnRead);
        chunkProvider.setSkipPolicy(readSkipPolicy);
        chunkProvider.setRollbackClassifier(getRollbackClassifier());
        chunkProvider.setInterval(options.getInterval());
        chunkProvider.setIdleTimeout(options.getIdleTimeout().orElse(null));
        ArrayList<StepListener> listeners = new ArrayList<>(getItemListeners());
        listeners.addAll(getSkipListeners());
        chunkProvider.setListeners(listeners);
        return chunkProvider;
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> chunk(int chunkSize) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.chunk(chunkSize);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> chunk(CompletionPolicy completionPolicy) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.chunk(completionPolicy);
    }

    public FlushingFaultTolerantStepBuilder<I, O> options(FlushingStepOptions options) {
        this.options = options;
        return this;
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> faultTolerant() {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.faultTolerant();
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> reader(ItemReader<? extends I> reader) {
        Assert.state(reader instanceof PollableItemReader, "Reader must be an instance of PollableItemReader");
        return (FlushingFaultTolerantStepBuilder<I, O>) super.reader(reader);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> writer(ItemWriter<? super O> writer) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.writer(writer);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> processor(Function<? super I, ? extends O> function) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.processor(function);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> processor(ItemProcessor<? super I, ? extends O> processor) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.processor(processor);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> readerIsTransactionalQueue() {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.readerIsTransactionalQueue();
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> listener(Object listener) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.listener(listener);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> listener(ItemReadListener<? super I> listener) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.listener(listener);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> listener(ItemWriteListener<? super O> listener) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.listener(listener);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> listener(ItemProcessListener<? super I, ? super O> listener) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.listener(listener);
    }

    @Override
    public FlushingFaultTolerantStepBuilder<I, O> chunkOperations(RepeatOperations repeatTemplate) {
        return (FlushingFaultTolerantStepBuilder<I, O>) super.chunkOperations(repeatTemplate);
    }

}
