package com.redis.spring.batch.reader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader.LiveReaderBuilder;
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

	private static final Log log = LogFactory.getLog(KeyspaceNotificationItemReader.class);

	public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";
	public static final KeyspaceNotificationOrderingStrategy DEFAULT_ORDERING = KeyspaceNotificationOrderingStrategy.PRIORITY;

	private static final String SEPARATOR = ":";
	private static final KeyspaceNotificationComparator NOTIFICATION_COMPARATOR = new KeyspaceNotificationComparator();

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private static final Map<String, KeyEventType> eventTypes = Stream.of(KeyEventType.values())
			.collect(Collectors.toMap(KeyEventType::getString, Function.identity()));

	private QueueOptions queueOptions = QueueOptions.builder().build();
	private List<String> patterns = LiveReaderBuilder.defaultPatterns();
	private Set<String> types = new HashSet<>();
	private KeyspaceNotificationOrderingStrategy prioritization = DEFAULT_ORDERING;

	private Publisher publisher;
	private BlockingQueue<KeyspaceNotification> queue;

	public KeyspaceNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
	}

	public KeyspaceNotificationItemReader<K, V> withQueueOptions(QueueOptions options) {
		this.queueOptions = options;
		return this;
	}

	public KeyspaceNotificationItemReader<K, V> withPatterns(List<String> patterns) {
		this.patterns = patterns;
		return this;
	}

	/**
	 * Set key types to include. Empty means include all types.
	 * 
	 * @param types
	 */
	public KeyspaceNotificationItemReader<K, V> withTypes(List<String> types) {
		this.types = new HashSet<>(types);
		return this;
	}

	public KeyspaceNotificationItemReader<K, V> withTypes(String... types) {
		return withTypes(Arrays.asList(types));
	}

	public KeyspaceNotificationItemReader<K, V> withOrderingStrategy(KeyspaceNotificationOrderingStrategy strategy) {
		this.prioritization = strategy;
		return this;
	}

	public BlockingQueue<KeyspaceNotification> getQueue() {
		return queue;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (publisher == null) {
			queue = new SetBlockingQueue<>(notificationQueue());
			Utils.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
			publisher = publisher();
			log.debug("Subscribing to keyspace notifications");
			publisher.open(patterns);
		}
	}

	private BlockingQueue<KeyspaceNotification> notificationQueue() {
		if (prioritization == KeyspaceNotificationOrderingStrategy.PRIORITY) {
			return new PriorityBlockingQueue<>(queueOptions.getCapacity(), NOTIFICATION_COMPARATOR);
		}
		return new LinkedBlockingQueue<>(queueOptions.getCapacity());
	}

	private Publisher publisher() {
		StatefulRedisPubSubConnection<String, String> connection = RedisModulesUtils.pubSubConnection(client);
		if (connection instanceof StatefulRedisClusterPubSubConnection) {
			return new RedisClusterPublisher((StatefulRedisClusterPubSubConnection<String, String>) connection);
		}
		return new RedisPublisher(connection);
	}

	@Override
	public void close() {
		if (publisher != null) {
			log.debug("Unsubscribing from keyspace notifications");
			publisher.close(patterns);
			publisher = null;
		}
		super.close();
	}

	private void notification(String channel, String message) {
		if (channel != null) {
			String key = channel.substring(channel.indexOf(SEPARATOR) + 1);
			KeyEventType eventType = eventType(message);
			if (types.isEmpty() || types.contains(eventType.getType())) {
				KeyspaceNotification notification = new KeyspaceNotification(key, eventType);
				if (!queue.offer(notification)) {
					log.warn("Could not add key because queue is full");
				}
			}
		}
	}

	private KeyEventType eventType(String message) {
		return eventTypes.getOrDefault(message, KeyEventType.UNKNOWN);
	}

	@Override
	public K read() throws Exception {
		return poll(queueOptions.getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public K poll(long timeout, TimeUnit unit) throws InterruptedException {
		KeyspaceNotification notification = queue.poll(timeout, unit);
		if (notification == null) {
			return null;
		}
		return codec.decodeKey(StringCodec.UTF8.encodeKey(notification.getKey()));
	}

	private interface Publisher {

		void open(List<String> patterns);

		void close(List<String> patterns);
	}

	private class RedisPublisher extends RedisPubSubAdapter<String, String> implements Publisher {

		private final StatefulRedisPubSubConnection<String, String> connection;

		public RedisPublisher(StatefulRedisPubSubConnection<String, String> connection) {
			this.connection = connection;
		}

		@Override
		public void open(List<String> patterns) {
			connection.addListener(this);
			connection.sync().psubscribe(patterns.toArray(new String[0]));
		}

		@Override
		public void close(List<String> patterns) {
			connection.sync().punsubscribe(patterns.toArray(new String[0]));
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

	private class RedisClusterPublisher extends RedisClusterPubSubAdapter<String, String> implements Publisher {

		private final StatefulRedisClusterPubSubConnection<String, String> connection;

		public RedisClusterPublisher(StatefulRedisClusterPubSubConnection<String, String> connection) {
			this.connection = connection;
		}

		@Override
		public void open(List<String> patterns) {
			connection.addListener(this);
			connection.setNodeMessagePropagation(true);
			connection.sync().upstream().commands().psubscribe(patterns.toArray(new String[0]));
		}

		@Override
		public void close(List<String> patterns) {
			connection.sync().upstream().commands().punsubscribe(patterns.toArray(new String[0]));
			connection.removeListener(this);
			connection.close();
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
