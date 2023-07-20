package com.redis.spring.batch.common;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;

public class CompositeItemStreamProcessor<I, P, O> implements ItemStreamProcessor<I, O> {

    private final ItemProcessor<I, P> inputProcessor;

    private final ItemProcessor<P, O> pivotProcessor;

    public CompositeItemStreamProcessor(ItemProcessor<I, P> inputProcessor, ItemProcessor<P, O> pivotProcessor) {
        this.inputProcessor = inputProcessor;
        this.pivotProcessor = pivotProcessor;
    }

    public boolean isOpen() {
        return Utils.isOpen(inputProcessor) && Utils.isOpen(pivotProcessor);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (inputProcessor instanceof ItemStream) {
            ((ItemStream) inputProcessor).open(executionContext);
        }
        if (pivotProcessor instanceof ItemStream) {
            ((ItemStream) pivotProcessor).open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (inputProcessor instanceof ItemStream) {
            ((ItemStream) inputProcessor).update(executionContext);
        }
        if (pivotProcessor instanceof ItemStream) {
            ((ItemStream) pivotProcessor).update(executionContext);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        if (inputProcessor instanceof ItemStream) {
            ((ItemStream) inputProcessor).close();
        }
        if (pivotProcessor instanceof ItemStream) {
            ((ItemStream) pivotProcessor).close();
        }
    }

    @Override
    public O process(I item) throws Exception {
        P pivotItem = inputProcessor.process(item);
        if (pivotItem == null) {
            return null;
        }
        return pivotProcessor.process(pivotItem);
    }

}
