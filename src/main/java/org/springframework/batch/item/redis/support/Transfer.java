package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.util.Assert;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Transfer<I, O> implements TransferTaskListener {

    @Getter
    private final String name;
    private final ItemReader<I> reader;
    private final ItemWriter<? extends O> writer;
    private final List<TransferTask<I>> tasks;
    private final List<TransferListener> listeners = new ArrayList<>();
    private Long flushPeriod;

    public void setFlushPeriod(long flushPeriod) {
	this.flushPeriod = flushPeriod;
    }

    public void addListener(TransferListener listener) {
	this.listeners.add(listener);
    }

    public int available() {
	if (reader instanceof BoundedItemReader) {
	    return ((BoundedItemReader<I>) reader).available();
	}
	return 0;
    }

    @Builder
    public Transfer(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer, int threads,
	    int batch) {
	Assert.notNull(reader, "A reader instance is required.");
	Assert.notNull(writer, "A writer instance is required.");
	Assert.isTrue(threads > 0, "Thread count must be greater than 0.");
	Assert.isTrue(batch > 0, "Batch size must be greater than 0.");
	this.name = name;
	this.reader = reader;
	this.writer = writer;
	this.tasks = new ArrayList<>(threads);
	for (int index = 0; index < threads; index++) {
	    this.tasks.add(new TransferTask<>(reader(), writer(processor, writer), batch));
	}
    }

    private void open(ExecutionContext executionContext) {
	if (writer instanceof ItemStream) {
	    log.debug("Opening writer");
	    ((ItemStream) writer).open(executionContext);
	}
	if (reader instanceof ItemStream) {
	    log.debug("Opening reader");
	    ((ItemStream) reader).open(executionContext);
	}
    }

    private void close() {
	if (reader instanceof ItemStream) {
	    log.debug("Closing reader");
	    ((ItemStream) reader).close();
	}
	if (writer instanceof ItemStream) {
	    log.debug("Closing writer");
	    ((ItemStream) writer).close();
	}
    }

    @SuppressWarnings("unchecked")
    private ItemWriter<I> writer(ItemProcessor<I, O> processor, ItemWriter<O> writer) {
	if (processor == null) {
	    return (ItemWriter<I>) writer;
	}
	return new ProcessingItemWriter<>(processor, writer);
    }

    public CompletableFuture<Void> executeAsync() {
	open(new ExecutionContext());
	List<CompletableFuture<Void>> futures = new ArrayList<>();
	for (TransferTask<I> task : tasks) {
	    task.addListener(this);
	    futures.add(CompletableFuture.runAsync(task));
	}
	CompletableFuture<Void> future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
	if (flushPeriod != null) {
	    ScheduledFuture<?> flushFuture = Executors.newSingleThreadScheduledExecutor()
		    .scheduleAtFixedRate(this::flush, flushPeriod, flushPeriod, TimeUnit.MILLISECONDS);
	    future.whenComplete((v, t) -> flushFuture.cancel(true));
	}
	future.whenComplete((v, t) -> close());
	return future;
    }

    private ItemReader<I> reader() {
	if (tasks.size() > 1 && reader instanceof ItemStreamReader) {
	    SynchronizedItemStreamReader<I> synchronizedReader = new SynchronizedItemStreamReader<>();
	    synchronizedReader.setDelegate((ItemStreamReader<I>) reader);
	    return synchronizedReader;
	}
	return reader;
    }

    private void flush() {
	if (reader instanceof AbstractKeyValueItemReader) {
	    ((AbstractKeyValueItemReader<I>) reader).flush();
	}
	for (TransferTask<I> task : tasks) {
	    try {
		task.flush();
	    } catch (Exception e) {
		log.error("Could not flush", e);
	    }
	}
    }

    @Override
    public void onUpdate(long count) {
	long totalCount = tasks.stream().mapToLong(TransferTask::getCount).sum();
	listeners.forEach(l -> l.onUpdate(totalCount));
    }

}
