package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.Utils;

public abstract class AbstractKeyspaceNotificationItemReader<K> extends ItemStreamSupport
		implements PollableItemReader<K> {

	private final Log log = LogFactory.getLog(getClass());

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_DEFAULT_QUEUE_POLL_TIMEOUT = Duration.ofMillis(100);

	private final Collection<Listener<K>> listeners = new ArrayList<>();
	private final Converter<K, K> keyExtractor;
	protected final K[] patterns;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private Duration defaultQueuePollTimeout = DEFAULT_DEFAULT_QUEUE_POLL_TIMEOUT;
	private boolean open;

	private BlockingQueue<K> queue;

	protected AbstractKeyspaceNotificationItemReader(Converter<K, K> keyExtractor, K[] patterns) {
		Assert.notNull(patterns, "Patterns must not be null");
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

	public void addListener(Listener<K> listener) {
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
			log.warn("Could not add key because queue is full");
		}
	}

	@Override
	public K read() throws Exception {
		return poll(defaultQueuePollTimeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (queue == null) {
			this.queue = new LinkedBlockingQueue<>(queueCapacity);
			Utils.createGaugeCollectionSize("reader.notification.queue.size", queue);
			doOpen();
			this.open = true;
		}
	}

	protected abstract void doOpen();

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (queue == null) {
			return;
		}
		if (!queue.isEmpty()) {
			log.warn("Closing with items still in queue");
		}
		doClose();
		queue = null;
		this.open = false;
	}

	public boolean isOpen() {
		return open;
	}

	protected abstract void doClose();

	public static interface Listener<K> {

		void key(K key);

	}

}
