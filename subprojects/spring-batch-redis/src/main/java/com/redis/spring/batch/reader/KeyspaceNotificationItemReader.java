package com.redis.spring.batch.reader;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public class KeyspaceNotificationItemReader<K> extends ItemStreamSupport
		implements PollableItemReader<K>, KeyspaceNotificationListener<K> {

	private final KeyspaceNotificationPublisher<K> publisher;
	private final Converter<K, K> keyExtractor;
	protected final K[] patterns;
	private final QueueOptions queueOptions;
	private final KeyQueue<K> queue;

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
		this.queue = new DedupingQueue<>(queueOptions);
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
		if (!filter.test(key)) {
			return;
		}
		queue.offer(key);
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
		publisher.addListener(this);
		publisher.subscribe(patterns);
		this.open = true;
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
		queue.clear();
		publisher.unsubscribe(patterns);
		this.open = false;
	}

	public boolean isOpen() {
		return open;
	}

}
