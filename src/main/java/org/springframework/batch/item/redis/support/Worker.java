package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.batch.core.metrics.BatchMetrics;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Worker<T> implements Runnable {

	private final String name;
	private final ItemReader<T> reader;
	private final ItemWriter<T> writer;
	private final int batch;
	private final List<T> items;
	private final Collection<WorkerListener> listeners = new ArrayList<>();
	@Getter
	private boolean stopped;

	public Worker(String name, ItemReader<T> reader, ItemWriter<T> writer, int batch) {
		this.name = name;
		this.reader = reader;
		this.writer = writer;
		this.batch = batch;
		this.items = new ArrayList<>(batch);
	}

	public void addListener(WorkerListener listener) {
		this.listeners.add(listener);
	}

	public void stop() {
		this.stopped = true;
	}

	@Override
	public void run() {
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
				log.info("Worker stopped");
				return;
			}
			flush();
			log.info("Worker completed");
		} catch (Exception e) {
			log.error("Could not transfer items", e);
			throw new RuntimeException(e);
		}
	}

	public void flush() {
		synchronized (items) {
			Timer.Sample sample = BatchMetrics.createTimerSample();
			String status = BatchMetrics.STATUS_SUCCESS;
			try {
				writer.write(items);
				listeners.forEach(l -> l.onWrite(items.size()));
			} catch (Exception e) {
				status = BatchMetrics.STATUS_FAILURE;
				log.error("Could not write {} items", items.size(), e);
			} finally {
				items.clear();
				sample.stop(BatchMetrics.createTimer("redis.write", "Batch write duration",
						Tag.of("transfer.name", name), Tag.of("status", status)));
			}
		}
	}

	public interface WorkerListener {

		void onWrite(long count);

	}
}