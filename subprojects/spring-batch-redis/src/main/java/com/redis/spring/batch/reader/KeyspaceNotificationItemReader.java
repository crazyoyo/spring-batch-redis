package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.SetBlockingQueue;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class KeyspaceNotificationItemReader<K, V> extends AbstractItemStreamItemReader<K>
		implements PollableItemReader<K> {

	public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

	public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";
	private static final String SEPARATOR = ":";
	private static final KeyspaceNotificationComparator NOTIFICATION_COMPARATOR = new KeyspaceNotificationComparator();
	private static final Map<String, KeyEventType> eventTypes = Stream.of(KeyEventType.values())
			.collect(Collectors.toMap(KeyEventType::getString, Function.identity()));

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final List<KeyspaceNotificationListener> listeners = new ArrayList<>();

	private KeyspaceNotificationOptions options = KeyspaceNotificationOptions.builder().build();

	private BlockingQueue<KeyspaceNotification> queue;
	private KeyspaceNotificationPublisher publisher;

	public KeyspaceNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
	}

	public void addListener(KeyspaceNotificationListener listener) {
		this.listeners.add(listener);
	}

	public KeyspaceNotificationOptions getOptions() {
		return options;
	}

	public void setOptions(KeyspaceNotificationOptions options) {
		this.options = options;
	}

	public BlockingQueue<KeyspaceNotification> getQueue() {
		return queue;
	}

	private String pattern(int database, String match) {
		return String.format(PUBSUB_PATTERN_FORMAT, database, match);
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (publisher == null) {
			queue = new SetBlockingQueue<>(notificationQueue());
			Utils.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
			publisher = publisher();
		}
	}

	private BlockingQueue<KeyspaceNotification> notificationQueue() {
		if (options.getOrderingStrategy() == KeyspaceNotificationOrderingStrategy.PRIORITY) {
			return new PriorityBlockingQueue<>(options.getQueueOptions().getCapacity(), NOTIFICATION_COMPARATOR);
		}
		return new LinkedBlockingQueue<>(options.getQueueOptions().getCapacity());
	}

	private KeyspaceNotificationPublisher publisher() {
		String pattern = pattern(options.getDatabase(), options.getMatch());
		if (client instanceof RedisModulesClusterClient) {
			return new RedisClusterKeyspaceNotificationPublisher(((RedisModulesClusterClient) client).connectPubSub(),
					pattern);
		}
		return new RedisKeyspaceNotificationPublisher(((RedisModulesClient) client).connectPubSub(), pattern);
	}

	@Override
	public void close() {
		if (publisher != null) {
			publisher.close();
			publisher = null;
		}
		super.close();
	}

	private void notification(String channel, String message) {
		if (channel != null) {
			String key = channel.substring(channel.indexOf(SEPARATOR) + 1);
			KeyEventType eventType = eventType(message);
			Optional<String> type = options.getType();
			if (!type.isPresent() || type.get().equals(eventType.getType())) {
				KeyspaceNotification notification = new KeyspaceNotification(key, eventType);
				if (!queue.offer(notification)) {
					listeners.forEach(l -> l.queueFull(notification));
				}
			}
		}
	}

	private KeyEventType eventType(String message) {
		return eventTypes.getOrDefault(message, KeyEventType.UNKNOWN);
	}

	@Override
	public K read() throws Exception {
		return poll(options.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		KeyspaceNotification notification = queue.poll(timeout, unit);
		if (notification == null) {
			return null;
		}
		return codec.decodeKey(StringCodec.UTF8.encodeKey(notification.getKey()));
	}

	private interface KeyspaceNotificationPublisher extends AutoCloseable {

		@Override
		void close();

	}

	private class RedisKeyspaceNotificationPublisher extends RedisPubSubAdapter<String, String>
			implements KeyspaceNotificationPublisher {

		private final StatefulRedisPubSubConnection<String, String> connection;
		private final String pattern;

		public RedisKeyspaceNotificationPublisher(StatefulRedisPubSubConnection<String, String> connection,
				String pattern) {
			this.connection = connection;
			this.pattern = pattern;
			connection.addListener(this);
			connection.sync().psubscribe(pattern);
		}

		@Override
		public void close() {
			connection.sync().punsubscribe(pattern);
			connection.removeListener(this);
			connection.close();
		}

		@Override
		public void message(String channel, String message) {
			notification(channel, message);
		}

		@Override
		public void message(String pattern, String channel, String message) {
			notification(channel, message);
		}

	}

	private class RedisClusterKeyspaceNotificationPublisher extends RedisClusterPubSubAdapter<String, String>
			implements KeyspaceNotificationPublisher {

		private final StatefulRedisClusterPubSubConnection<String, String> connection;
		private final String pattern;

		public RedisClusterKeyspaceNotificationPublisher(
				StatefulRedisClusterPubSubConnection<String, String> connection, String pattern) {
			this.connection = connection;
			this.pattern = pattern;
			connection.addListener(this);
			connection.setNodeMessagePropagation(true);
			connection.sync().upstream().commands().psubscribe(pattern);

		}

		@Override
		public void close() {
			connection.sync().upstream().commands().punsubscribe(pattern);
			connection.removeListener(this);
		}

		@Override
		public void message(RedisClusterNode node, String channel, String message) {
			notification(channel, message);
		}

		@Override
		public void message(RedisClusterNode node, String pattern, String channel, String message) {
			notification(channel, message);
		}
	}

}
