package org.springframework.batch.item.redis.support;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Batcher<K> implements Runnable {

    private final ItemReader<K> reader;
    private final int batchSize;
    private final ItemWriter<K> writer;
    private List<K> items;
    private boolean stopped;

    @Builder
    public Batcher(@NonNull ItemReader<K> reader, @NonNull ItemWriter<K> writer, int batchSize) {
        this.reader = reader;
        this.writer = writer;
        this.batchSize = batchSize;
        this.items = new ArrayList<>(batchSize);
    }

    public void stop() {
        this.stopped = true;
    }

    @Override
    public void run() {
        K key;
        try {
            while ((key = reader.read()) != null && !stopped) {
                synchronized (items) {
                    items.add(key);
                }
                if (items.size() >= batchSize) {
                    flush();
                }
            }
            flush();
        } catch (Exception e) {
            log.error("Could not batch values", e);
        }
    }

    public void flush() throws Exception {
        List<K> snapshot = snapshot();
        writer.write(snapshot);
    }

    private List<K> snapshot() {
        synchronized (items) {
            List<K> snapshot = new ArrayList<>(items);
            items.clear();
            return snapshot;
        }
    }

}