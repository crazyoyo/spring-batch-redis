package org.springframework.batch.item.redis.support;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BatchRunnable<T> implements Runnable {

    private final ItemReader<T> reader;
    private final List<T> items;
    private final int batchSize;
    private final ItemWriter<T> writer;
    private final List<Listener> listeners = new ArrayList<>();

    @Getter
    private long writeCount;
    private boolean stopped;

    public BatchRunnable(ItemReader<T> reader, ItemWriter<T> writer, int batchSize) {
        this.reader = reader;
        this.writer = writer;
        this.batchSize = batchSize;
        this.items = new ArrayList<>(batchSize);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void stop() {
        this.stopped = true;
    }

    @Override
    public void run() {
        this.writeCount = 0;
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
            log.error("Could not read values", e);
        }
    }

    public void flush() throws Exception {
        synchronized (items) {
            write(items);
            writeCount += items.size();
            items.clear();
            listeners.forEach(l -> l.onWrite(writeCount));
        }
    }

    protected void write(List<T> items) throws Exception {
        writer.write(items);
    }

    public interface Listener {

        void onWrite(long writeCount);
    }
}