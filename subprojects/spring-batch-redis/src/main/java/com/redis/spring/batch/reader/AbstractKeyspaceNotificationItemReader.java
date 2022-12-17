package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

public abstract class AbstractKeyspaceNotificationItemReader<K> extends AbstractItemCountingItemStreamItemReader<K>
		implements PollableItemReader<K> {

	private final Log log = LogFactory.getLog(getClass());
	private final Converter<K, K> keyExtractor;
	private final K[] patterns;
	private final BlockingQueue<K> queue;
	private boolean open;

	protected AbstractKeyspaceNotificationItemReader(K[] patterns, Converter<K, K> keyExtractor,
			BlockingQueue<K> queue) {
		Assert.notNull(patterns, "Patterns must not be null");
		setName(ClassUtils.getShortName(getClass()));
		this.patterns = patterns;
		this.keyExtractor = keyExtractor;
		this.queue = queue;
	}

	@Override
	protected synchronized void doOpen() {
		if (open) {
			return;
		}
		log.debug("Subscribing to keyspace notifications");
		open(patterns);
		this.open = true;
	}

	protected abstract void open(K[] patterns);

	@Override
	protected synchronized void doClose() {
		if (!open) {
			return;
		}
		log.debug("Unsubscribing from keyspace notifications");
		close(patterns);
		this.open = false;
	}

	protected abstract void close(K[] patterns);

	@Override
	public boolean isOpen() {
		return open;
	}

	public void notification(K notification) {
		if (notification == null) {
			return;
		}
		K key = keyExtractor.convert(notification);
		if (!queue.offer(key)) {
			log.warn("Could not add key because queue is full");
		}
	}

	@Override
	protected K doRead() throws Exception {
		return poll(QueueOptions.DEFAULT_POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

}
