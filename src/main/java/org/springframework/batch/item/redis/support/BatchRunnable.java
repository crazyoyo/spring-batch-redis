package org.springframework.batch.item.redis.support;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BatchRunnable<I> implements Runnable {

    private final ItemReader<I> reader;
    private final List<I> items;
    private final int batchSize;
    private final ItemWriter<I> writer;
    private final List<Listener> listeners = new ArrayList<>();

    @Getter
    private long writeCount;

    public BatchRunnable(ItemReader<I> reader, ItemWriter<I> writer, int batchSize) {
        this.reader = reader;
        this.writer = writer;
        this.batchSize = batchSize;
        this.items = new ArrayList<>(batchSize);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void run() {
        this.writeCount = 0;
        try {
            I item;
            while ((item = reader.read()) != null) {
                synchronized (items) {
                    items.add(item);
                }
                if (items.size() >= batchSize) {
                    flush();
                }
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

    protected void write(List<I> items) throws Exception {
        writer.write(items);
    }

    public interface Listener {

        void onWrite(long writeCount);
    }
}