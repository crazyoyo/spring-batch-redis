package com.redis.spring.batch.reader;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.Utils;

public class KeyspaceNotificationItemReader<K> extends ItemStreamSupport
		implements PollableItemReader<K>, KeyspaceNotificationListener<K> {

	public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";

	private final Log log = LogFactory.getLog(getClass());

	private final KeyspaceNotificationPublisher<K> publisher;
	private final Converter<K, K> keyExtractor;
	protected final K[] patterns;
	private final QueueOptions queueOptions;
	private final Set<K> set;
	private final BlockingQueue<K> queue;
	private Predicate<K> filter = k -> true;

	private boolean open;

	public KeyspaceNotificationItemReader(KeyspaceNotificationPublisher<K> publisher, Converter<K, K> keyExtractor,
			K[] patterns, QueueOptions queueOptions) {
		Assert.notNull(patterns, "Patterns must not be null");
		setName(ClassUtils.getShortName(getClass()));
		this.publisher = publisher;
		this.keyExtractor = keyExtractor;
		this.patterns = patterns;
		this.queueOptions = queueOptions;
		this.set = new HashSet<>();
		this.queue = new LinkedBlockingQueue<>(queueOptions.getCapacity());
	}

	public void setFilter(Predicate<K> filter) {
		this.filter = filter;
	}

	@Override
	public void notification(K notification) {
		if (notification == null) {
			return;
		}
		K key = keyExtractor.convert(notification);
		if (set.contains(key)) {
			return;
		}
		if (!filter.test(key)) {
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
	public K read() throws Exception {
		return poll(queueOptions.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (open) {
			return;
		}
		Utils.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
		publisher.addListener(this);
		publisher.subscribe(patterns);
		this.open = true;
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		K key = queue.poll(timeout, unit);
		if (key == null) {
			return null;
		}
		set.remove(key);
		return key;
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (!open) {
			return;
		}
		if (!queue.isEmpty()) {
			log.warn("Closing with items still in queue");
			queue.clear();
		}
		set.clear();
		publisher.unsubscribe(patterns);
		this.open = false;
	}

	public boolean isOpen() {
		return open;
	}

}
