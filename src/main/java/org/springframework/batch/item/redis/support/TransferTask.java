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
public class TransferTask<T> implements Runnable {

    private final ItemReader<T> reader;
    private final List<T> items;
    private final int batch;
    private final ItemWriter<T> writer;
    private final List<TransferTaskListener> listeners = new ArrayList<>();
    @Getter
    private long count;
    @Getter
    private boolean stopped;

    @Builder
    public TransferTask(ItemReader<T> reader, ItemWriter<T> writer, int batch) {
	this.reader = reader;
	this.writer = writer;
	this.batch = batch;
	this.items = new ArrayList<>(batch);
    }

    public void addListener(TransferTaskListener listener) {
	listeners.add(listener);
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
		if (items.size() >= batch) {
		    flush();
		}
	    }
	    if (stopped) {
		log.info("BatchTransfer stopped");
		return;
	    }
	    flush();
	    log.info("{} complete - {} items transferred", ClassUtils.getShortName(getClass()), count);
	} catch (Exception e) {
	    log.error("Could not transfer items", e);
	    throw new RuntimeException(e);
	}
    }

    public void flush() throws Exception {
	synchronized (items) {
	    writer.write(items);
	    count += items.size();
	    listeners.forEach(l -> l.onUpdate(count));
	    items.clear();
	}
    }

}
