package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.ClassUtils;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchTransfer<T> implements Runnable, ProgressReporter {

    private final ItemReader<T> reader;
    private final List<T> items;
    private final int batchSize;
    private final ItemWriter<T> writer;
    private long count;
    @Getter
    private boolean stopped;

    @Builder
    public BatchTransfer(ItemReader<T> reader, ItemWriter<T> writer, int batchSize) {
	this.reader = reader;
	this.writer = writer;
	this.batchSize = batchSize;
	this.items = new ArrayList<>(batchSize);
    }

    @Override
    public long getDone() {
	return count;
    }

    @Override
    public Long getTotal() {
	if (reader instanceof ProgressReporter) {
	    return ((ProgressReporter) reader).getTotal();
	}
	return null;
    }

    public void stop() {
	this.stopped = true;
    }

    @Override
    public void run() {
	count = 0;
	try {
	    T item;
	    while ((item = reader.read()) != null && !stopped) {
		synchronized (items) {
		    items.add(item);
		}
		if (items.size() >= batchSize) {
		    count += flush();
		}
	    }
	    if (stopped) {
		log.info("BatchTransfer stopped");
		return;
	    }
	    count += flush();
	    log.info("{} complete - {} items transferred", ClassUtils.getShortName(getClass()), count);
	} catch (Exception e) {
	    log.error("Could not transfer items", e);
	    throw new RuntimeException(e);
	}
    }

    public int flush() throws Exception {
	synchronized (items) {
	    writer.write(items);
	    int count = items.size();
	    items.clear();
	    return count;
	}
    }

}
