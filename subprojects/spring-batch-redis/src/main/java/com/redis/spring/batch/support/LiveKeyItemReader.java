package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiveKeyItemReader<K> extends ItemStreamSupport implements PollableItemReader<K> {

	private final MessageListener messageListener = new MessageListener();
	private final Collection<KeyListener<K>> listeners = new ArrayList<>();
	private final PubSubSubscriber<K> subscriber;
	private final BlockingQueue<K> queue;
	private final long defaultQueuePollTimeout;
	private final Converter<K, K> keyExtractor;
	private boolean open;

	protected LiveKeyItemReader(PubSubSubscriber<K> subscriber, BlockingQueue<K> queue,
			Duration defaultQueuePollTimeout, Converter<K, K> keyExtractor) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(subscriber, "A subscriber is required");
		Assert.notNull(queue, "A queue is required");
		Utils.assertPositive(defaultQueuePollTimeout, "Default queue poll timeout");
		Assert.notNull(keyExtractor, "A key extractor is required");
		this.subscriber = subscriber;
		this.queue = queue;
		this.defaultQueuePollTimeout = defaultQueuePollTimeout.toMillis();
		this.keyExtractor = keyExtractor;
	}

	public void addListener(KeyListener<K> listener) {
		this.listeners.add(listener);
	}

	@Override
	public K read() throws Exception {
		return poll(defaultQueuePollTimeout, TimeUnit.MILLISECONDS);
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (open) {
			return;
		}
		Utils.createGaugeCollectionSize("reader.notification.queue.size", queue);
		subscriber.open(messageListener);
		open = true;
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (!open) {
			return;
		}
		subscriber.close();
		open = false;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	private class MessageListener implements KeyMessageListener<K> {

		@Override
		public void message(K message) {
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

	}

	public static interface KeyListener<K> {

		void key(K key);

	}

}
