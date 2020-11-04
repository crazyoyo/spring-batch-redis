package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyValueItemReader<K, T> extends AbstractItemCountingItemStreamItemReader<T> {

    @Getter
    private final ItemReader<K> keyReader;

    @Getter
    private final ItemProcessor<List<? extends K>, List<T>> valueReader;

    private final BlockingQueue<T> itemQueue;

    private final ExecutorService executor;

    private final List<BatchTransfer<K>> enqueuers;

    private final long queuePollingTimeout;

    public KeyValueItemReader(ItemReader<K> keyReader, ItemProcessor<List<? extends K>, List<T>> valueReader,
	    int threadCount, int batchSize, int queueCapacity, long queuePollingTimeout) {
	setName(ClassUtils.getShortName(getClass()));
	Assert.notNull(keyReader, "A key reader is required.");
	Assert.notNull(valueReader, "A value reader is required.");
	this.keyReader = keyReader;
	this.valueReader = valueReader;
	this.itemQueue = new LinkedBlockingDeque<>(queueCapacity);
	this.queuePollingTimeout = queuePollingTimeout;
	this.executor = Executors.newFixedThreadPool(threadCount);
	this.enqueuers = new ArrayList<>(threadCount);
	for (int index = 0; index < threadCount; index++) {
	    enqueuers.add(new BatchTransfer<K>(keyReader, this::write, batchSize));
	}
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
	if (keyReader instanceof ItemStream) {
	    ((ItemStream) keyReader).open(executionContext);
	}
	super.open(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
	super.close();
	if (keyReader instanceof ItemStream) {
	    ((ItemStream) keyReader).close();
	}
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
	if (keyReader instanceof ItemStream) {
	    ((ItemStream) keyReader).update(executionContext);
	}
    }

    @Override
    protected void doOpen() {
	enqueuers.forEach(executor::submit);
	executor.shutdown();
    }

    private void write(List<? extends K> keys) throws Exception {
	List<T> values = valueReader.process(keys);
	if (values == null) {
	    return;
	}
	itemQueue.addAll(values);
    }

    @Override
    protected void doClose() throws ItemStreamException {
	if (executor.isTerminated()) {
	    return;
	}
	executor.shutdownNow();
    }

    public void flush() {
	for (BatchTransfer<K> enqueuer : enqueuers) {
	    try {
		enqueuer.flush();
	    } catch (Exception e) {
		log.error("Could not flush", e);
	    }
	}
    }

    @Override
    protected T doRead() throws Exception {
	T item;
	do {
	    item = itemQueue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
	} while (item == null && !executor.isTerminated());
	return item;
    }

}
