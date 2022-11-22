package com.redis.spring.batch.reader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.common.queue.ConcurrentSetBlockingQueue;

public class KeyspaceNotificationItemReader<K> extends AbstractItemStreamItemReader<K>
		implements PollableItemReader<K>, KeyspaceNotificationListener<K> {

	public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";

	private final Log log = LogFactory.getLog(getClass());
	private final KeyspaceNotificationPublisher<K> publisher;
	private final Converter<K, K> keyExtractor;
	private final K[] patterns;
	private final QueueOptions queueOptions;
	private BlockingQueue<KeyWrapper<K>> queue;

	private Predicate<K> filter = k -> true;

	public KeyspaceNotificationItemReader(KeyspaceNotificationPublisher<K> publisher, Converter<K, K> keyExtractor,
			K[] patterns, QueueOptions queueOptions) {
		Assert.notNull(patterns, "Patterns must not be null");
		setName(ClassUtils.getShortName(getClass()));
		this.publisher = publisher;
		this.keyExtractor = keyExtractor;
		this.patterns = patterns;
		this.queueOptions = queueOptions;
	}

	public void setFilter(Predicate<K> filter) {
		this.filter = filter;
	}

	@Override
	public boolean notification(K notification) {
		if (notification == null) {
			return false;
		}
		K key = keyExtractor.convert(notification);
		if (filter.test(key)) {
			boolean added = queue.offer(new KeyWrapper<>(key));
			if (!added) {
				log.warn("Could not enqueue key");
			}
			return added;
		}
		return false;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (queue == null) {
			if (filter instanceof ItemStream) {
				((ItemStream) filter).open(executionContext);
			}
			this.queue = new ConcurrentSetBlockingQueue<>(queueOptions.getCapacity());
			Utils.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
			publisher.addListener(this);
			publisher.subscribe(patterns);
		}
		super.open(executionContext);
	}

	@Override
	public boolean isOpen() {
		return queue != null;
	}

	@Override
	public synchronized void update(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		if (filter instanceof ItemStream) {
			((ItemStream) filter).update(executionContext);
		}
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		super.close();
		if (queue != null) {
			if (!queue.isEmpty()) {
				log.warn("Closing with items still in queue");
			}
			queue = null;
			publisher.unsubscribe(patterns);
			if (filter instanceof ItemStream) {
				((ItemStream) filter).close();
			}
		}
	}

	@Override
	public K read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return poll(queueOptions.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		KeyWrapper<K> wrapper = queue.poll(timeout, unit);
		if (wrapper == null) {
			return null;
		}
		return wrapper.getKey();
	}

}
