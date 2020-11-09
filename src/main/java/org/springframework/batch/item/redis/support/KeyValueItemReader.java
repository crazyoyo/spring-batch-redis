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
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeyValueItemReader<K, V, T> extends AbstractProgressReportingItemReader<T> {

    @Getter
    private final ItemReader<K> keyReader;
    @Getter
    private final ItemProcessor<List<? extends K>, List<T>> valueReader;
    private final BlockingQueue<T> queue;
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
	this.queue = new LinkedBlockingDeque<>(queueCapacity);
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
    protected void doOpen() {
	enqueuers.forEach(executor::submit);
	executor.shutdown();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
	if (keyReader instanceof ItemStream) {
	    ((ItemStream) keyReader).update(executionContext);
	}
	super.update(executionContext);
    }

    @Override
    protected T doRead() throws Exception {
	T item;
	do {
	    item = queue.poll(queuePollingTimeout, TimeUnit.MILLISECONDS);
	} while (item == null && !executor.isTerminated());
	if (item == null) {
	    log.info("Read null value - {} items in queue, executor terminated: {}", queue.size(),
		    executor.isTerminated());
	}
	return item;
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

    private void write(List<? extends K> keys) throws Exception {
	List<T> values = valueReader.process(keys);
	if (values == null) {
	    return;
	}
	for (T value : values) {
	    queue.put(value);
	}
    }

    @Override
    public void close() throws ItemStreamException {
	if (!queue.isEmpty()) {
	    log.warn("Closing {} - {} items still in queue", ClassUtils.getShortName(getClass()), queue.size());
	}
	super.close();
	if (keyReader instanceof ItemStream) {
	    ((ItemStream) keyReader).close();
	}
    }

    @Override
    protected void doClose() throws ItemStreamException {
	if (executor.isTerminated()) {
	    return;
	}
	executor.shutdownNow();
    }

    @Override
    public Long getTotal() {
	if (keyReader instanceof ProgressReporter) {
	    return ((ProgressReporter) keyReader).getTotal();
	}
	return super.getTotal();
    }

}
