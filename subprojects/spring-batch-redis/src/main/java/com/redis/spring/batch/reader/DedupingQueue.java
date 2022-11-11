package com.redis.spring.batch.reader;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redis.spring.batch.common.Utils;

public class DedupingQueue<K> implements KeyQueue<K> {

	public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";

	private final Log log = LogFactory.getLog(getClass());
	private final Set<K> set;
	private final BlockingQueue<K> queue;

	public DedupingQueue(QueueOptions options) {
		this.set = new HashSet<>();
		this.queue = new LinkedBlockingQueue<>(options.getCapacity());
		Utils.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
	}

	@Override
	public void offer(K key) {
		if (set.contains(key)) {
			return;
		}
		boolean result = queue.offer(key);
		if (result) {
			set.add(key);
		} else {
			log.warn("Could not add key because queue is full");
		}
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		K key = queue.poll(timeout, unit);
		if (key != null) {
			set.remove(key);
		}
		return key;
	}

	@Override
	public void clear() {
		if (!queue.isEmpty()) {
			log.warn("Closing with items still in queue");
		}
		queue.clear();
		set.clear();

	}

}
