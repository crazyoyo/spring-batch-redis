package com.redis.spring.batch.support;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractKeyspaceNotificationItemReader<C extends StatefulRedisPubSubConnection<String, String>>
		extends ItemStreamSupport implements PollableItemReader<String> {

	private final Supplier<C> connectionSupplier;
	private final BlockingQueue<String> queue;
	private final List<String> pubSubPatterns;
	private final Set<String> elements;
	private C connection;
	private State state;
	private long defaultQueuePollTimeout;

	protected AbstractKeyspaceNotificationItemReader(Supplier<C> connectionSupplier, List<String> pubSubPatterns,
			int queueCapacity, Duration defaultQueuePollTimeout) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(connectionSupplier, "A pub/sub connection supplier is required");
		Assert.isTrue(queueCapacity > 0, "Queue capacity must be greater than zero");
		Assert.notEmpty(pubSubPatterns, "A pub/sub pattern is required");
		Utils.assertPositive(defaultQueuePollTimeout, "Default queue poll timeout");
		this.connectionSupplier = connectionSupplier;
		this.queue = new LinkedBlockingQueue<>(queueCapacity);
		this.elements = new HashSet<>(queueCapacity);
		this.pubSubPatterns = pubSubPatterns;
		this.defaultQueuePollTimeout = defaultQueuePollTimeout.toMillis();
	}

	@Override
	public String read() throws Exception {
		return poll(defaultQueuePollTimeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
		if (connection == null) {
			Utils.createGaugeCollectionSize("reader.notification.queue.size", queue);
			log.info("Connecting to Redis pub/sub");
			this.connection = connectionSupplier.get();
			log.info("Subscribing to {}", pubSubPatterns);
			subscribe(connection, pubSubPatterns);
			state = State.OPEN;
		}
	}

	@Override
	public State getState() {
		return state;
	}

	protected abstract void subscribe(C connection, List<String> patterns);

	@Override
	public String poll(long timeout, TimeUnit unit) throws InterruptedException {
		String key = queue.poll(timeout, unit);
		if (key == null) {
			return null;
		}
		elements.remove(key);
		return key;
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (connection == null) {
			return;
		}
		log.info("Unsubscribing from {}", pubSubPatterns);
		unsubscribe(connection, pubSubPatterns);
		connection.close();
		connection = null;
		state = State.CLOSED;
	}

	protected abstract void unsubscribe(C connection, List<String> patterns);

	protected void add(String message) {
		if (message == null) {
			return;
		}
		String key = message.substring(message.indexOf(":") + 1);
		if (key == null) {
			return;
		}
		if (elements.contains(key)) {
			return;
		}
		if (queue.offer(key)) {
			elements.add(key);
		} else {
			log.debug("Could not add key '{}': notification queue full (size={})", key, queue.size());
		}
	}

}
