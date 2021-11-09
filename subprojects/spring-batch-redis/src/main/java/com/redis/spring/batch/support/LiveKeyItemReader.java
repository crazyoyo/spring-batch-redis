package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ClassUtils;

public abstract class LiveKeyItemReader<K> extends ItemStreamSupport implements PollableItemReader<K> {

	private static final Logger log = LoggerFactory.getLogger(LiveKeyItemReader.class);

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);

	private final Collection<KeyListener<K>> listeners = new ArrayList<>();
	private final Converter<K, K> keyExtractor;
	protected final List<K> patterns;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private Duration defaultQueuePollTimeout = DEFAULT_DEFAULT_QUEUE_POLL_TIMEOUT;

	private boolean open;
	private BlockingQueue<K> queue;

	protected LiveKeyItemReader(Converter<K, K> keyExtractor, List<K> patterns) {
		setName(ClassUtils.getShortName(getClass()));
		this.keyExtractor = keyExtractor;
		this.patterns = patterns;
	}

	public void setQueueCapacity(int queueCapacity) {
		Utils.assertPositive(queueCapacity, "Queue capacity");
		this.queueCapacity = queueCapacity;
	}

	public void setDefaultQueuePollTimeout(Duration defaultQueuePollTimeout) {
		Utils.assertPositive(defaultQueuePollTimeout, "Default queue poll timeout");
		this.defaultQueuePollTimeout = defaultQueuePollTimeout;
	}

	public void addListener(KeyListener<K> listener) {
		this.listeners.add(listener);
	}

	protected void message(K message) {
		if (message == null) {
			return;
		}
		K key = keyExtractor.convert(message);
		if (key == null) {
			return;
		}
		listeners.forEach(l -> l.key(key));
		if (!queue.offer(key)) {
			log.debug("Could not add key: queue full (size={})", queue.size());
		}
	}

	@Override
	public K read() throws Exception {
		return poll(defaultQueuePollTimeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (open) {
			return;
		}
		this.queue = new LinkedBlockingQueue<>(queueCapacity);
		Utils.createGaugeCollectionSize("reader.notification.queue.size", queue);
		doOpen();
		open = true;
	}

	protected abstract void doOpen();

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (!open) {
			return;
		}
		doClose();
		open = false;
	}

	protected abstract void doClose();

	@Override
	public boolean isOpen() {
		return open;
	}

	public static interface KeyListener<K> {

		void key(K key);

	}

}
