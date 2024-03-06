package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.redis.spring.batch.common.SetBlockingQueue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class KeyspaceNotificationItemReader<K> implements PollableItemReader<K> {

	public static final Duration DEFAULT_FLUSH_INTERVAL = Duration.ofMillis(50);
	public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMillis(Long.MAX_VALUE);
	public static final int DEFAULT_QUEUE_CAPACITY = 10000;
	public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

	private static final String SEPARATOR = ":";
	private static final Map<String, KeyEvent> eventMap = Stream.of(KeyEvent.values())
			.collect(Collectors.toMap(KeyEvent::getString, Function.identity()));

	private static final Log log = LogFactory.getLog(KeyspaceNotificationItemReader.class);

	private final AbstractRedisClient client;
	private final Function<String, K> keyEncoder;
	private final String pubSubPattern;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private String keyType;

	private BlockingQueue<K> queue;
	private AutoCloseable publisher;

	public KeyspaceNotificationItemReader(AbstractRedisClient client, RedisCodec<K, ?> codec, String pubSubPattern) {
		this.client = client;
		this.keyEncoder = CodecUtils.stringKeyFunction(codec);
		this.pubSubPattern = pubSubPattern;
	}

	public BlockingQueue<K> getQueue() {
		return queue;
	}

	public void keyspaceNotification(String key, String type) {
		if (keyType == null || keyType.equalsIgnoreCase(type)) {
			boolean added = queue.offer(keyEncoder.apply(key));
			if (!added) {
				log.warn("Dropped keyspace notification because queue is full");
			}
		}
	}

	public void setQueueCapacity(int capacity) {
		this.queueCapacity = capacity;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public void setPollTimeout(Duration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (queue == null) {
			queue = new SetBlockingQueue<>(new LinkedBlockingQueue<>(queueCapacity), queueCapacity);
		}
		if (publisher == null) {
			publisher = publisher();
		}
	}

	private AutoCloseable publisher() {
		if (client instanceof RedisClusterClient) {
			return new RedisClusterKeyspaceNotificationPublisher();
		}
		return new RedisKeyspaceNotificationPublisher();
	}

	@Override
	public synchronized void close() throws ItemStreamException {
		if (publisher != null) {
			try {
				publisher.close();
				publisher = null;
			} catch (Exception e) {
				throw new ItemStreamException("Could not stop publisher", e);
			}
		}
		queue = null;
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	@Override
	public K read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
	}

	private boolean notification(String channel, String message) {
		int index = channel.indexOf(SEPARATOR);
		if (index > 0) {
			String key = channel.substring(index + 1);
			KeyEvent event = eventMap.getOrDefault(message, KeyEvent.UNKNOWN);
			keyspaceNotification(key, event.getType().getString());
		}
		return false;
	}

	private class RedisKeyspaceNotificationPublisher extends RedisPubSubAdapter<String, String>
			implements AutoCloseable {

		private final StatefulRedisPubSubConnection<String, String> connection;

		public RedisKeyspaceNotificationPublisher() {
			connection = ((RedisClient) client).connectPubSub();
			connection.addListener(this);
			connection.sync().psubscribe(pubSubPattern);
		}

		@Override
		public void close() {
			if (connection.isOpen()) {
				connection.sync().punsubscribe(pubSubPattern);
				connection.removeListener(this);
				connection.close();
			}
		}

		@Override
		public void message(String pattern, String channel, String message) {
			notification(channel, message);
		}

	}

	private class RedisClusterKeyspaceNotificationPublisher extends RedisClusterPubSubAdapter<String, String>
			implements AutoCloseable {

		private final StatefulRedisClusterPubSubConnection<String, String> connection;

		public RedisClusterKeyspaceNotificationPublisher() {
			this.connection = ((RedisClusterClient) client).connectPubSub();
			this.connection.setNodeMessagePropagation(true);
			this.connection.addListener(this);
			this.connection.sync().upstream().commands().psubscribe(pubSubPattern);
		}

		@Override
		public void close() throws Exception {
			if (connection.isOpen()) {
				connection.sync().upstream().commands().punsubscribe(pubSubPattern);
				connection.removeListener(this);
				connection.close();
			}
		}

		@Override
		public void message(RedisClusterNode node, String pattern, String channel, String message) {
			notification(channel, message);
		}

	}

}