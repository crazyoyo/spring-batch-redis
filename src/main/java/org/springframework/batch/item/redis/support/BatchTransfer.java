package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import lombok.Builder;
import lombok.Getter;

public class BatchTransfer<T> implements Runnable {

    private final ItemReader<T> reader;

    private final List<T> items;

    private final int batchSize;

    private final ItemWriter<T> writer;

    private final List<BatchTransferListener> listeners = new ArrayList<>();

    @Getter
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

    public void addListener(BatchTransferListener listener) {
	listeners.add(listener);
    }

    public void stop() {
	this.stopped = true;
    }

    @Override
    public void run() {
	this.count = 0;
	try {
	    T item;
	    while ((item = reader.read()) != null && !stopped) {
		synchronized (items) {
		    items.add(item);
		}
		if (items.size() >= batchSize) {
		    flush();
		}
	    }
	    if (stopped) {
		return;
	    }
	    flush();
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }

    public void flush() throws Exception {
	synchronized (items) {
	    write(items);
	    count += items.size();
	    items.clear();
	    listeners.forEach(l -> l.onProgress(count));
	}
    }

    protected void write(List<T> items) throws Exception {
	writer.write(items);
    }

    public interface BatchTransferListener {

	void onProgress(long count);

    }

}
