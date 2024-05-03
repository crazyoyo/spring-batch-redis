package com.redis.spring.batch.reader;

import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.BatchUtils;
import com.redis.spring.batch.common.DataType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class KeyNotificationItemReader<K, V> extends AbstractPollableItemReader<K> {

	private static final String KEYSPACE_PATTERN = "__keyspace@%s__:%s";
	private static final String KEYEVENT_PATTERN = "__keyevent@%s__:*";

	private static final String SEPARATOR = ":";

	private final Log log = LogFactory.getLog(KeyNotificationItemReader.class);

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final Function<String, K> keyEncoder;
	private final Function<K, String> keyDecoder;
	private final Function<V, String> valueDecoder;

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private int database;
	private String keyPattern;
	private String keyType;

	private BlockingQueue<K> queue;
	private AutoCloseable publisher;
	private HashSet<Wrapper<K>> keySet;

	public KeyNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.keyEncoder = BatchUtils.stringKeyFunction(codec);
		this.keyDecoder = BatchUtils.toStringKeyFunction(codec);
		this.valueDecoder = BatchUtils.toStringValueFunction(codec);
	}

	public BlockingQueue<K> getQueue() {
		return queue;
	}

	public String pubSubPattern() {
		if (isKeyEvents()) {
			return String.format(KEYEVENT_PATTERN, database);
		}
		return String.format(KEYSPACE_PATTERN, database, keyPattern);
	}

	private boolean isKeyEvents() {
		return keyPattern == null;
	}

	@Override
	public boolean isRunning() {
		return publisher != null;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		Assert.notNull(client, "Redis client not set");
		if (keySet == null) {
			keySet = new HashSet<>(queueCapacity);
		}
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueCapacity);
		}
		if (publisher == null) {
			publisher = publisher();
		}
	}

	private void keySpaceNotification(K channel, V message) {
		addEvent(keyEncoder.apply(suffix(channel)), valueDecoder.apply(message));
	}

	@SuppressWarnings("unchecked")
	private void keyEventNotification(K channel, V message) {
		addEvent((K) message, suffix(channel));
	}

	private void addEvent(K key, String event) {
		DataType type = keyType(event);
		if (keyType == null || keyType.equalsIgnoreCase(type.getString())) {
			Wrapper<K> wrapper = new Wrapper<>(key);
			if (keySet.contains(wrapper)) {
				return;
			}
			boolean added = queue.offer(key);
			if (added) {
				keySet.add(wrapper);
			}
		}
	}

	private NotificationConsumer<K, V> notificationConsumer() {
		if (isKeyEvents()) {
			return this::keyEventNotification;
		}
		return this::keySpaceNotification;
	}

	private String suffix(K key) {
		String string = keyDecoder.apply(key);
		int index = string.indexOf(SEPARATOR);
		if (index > 0) {
			return string.substring(index + 1);
		}
		return null;
	}

	private AutoCloseable publisher() {
		String pubSubPattern = pubSubPattern();
		K pattern = keyEncoder.apply(pubSubPattern);
		NotificationConsumer<K, V> consumer = notificationConsumer();
		if (client instanceof RedisClusterClient) {
			RedisClusterPubSubListener<K, V> listener = new ClusterKeyNotificationListener<>(consumer);
			return new RedisClusterKeyNotificationPublisher<>((RedisClusterClient) client, codec, listener, pattern);
		}
		RedisPubSubListener<K, V> listener = new KeyNotificationListener<>(consumer);
		return new RedisKeyNotificationPublisher<>((RedisClient) client, codec, listener, pattern);
	}

	private interface NotificationConsumer<K, V> {

		void accept(K channel, V message);

	}

	private static class KeyNotificationListener<K, V> extends RedisPubSubAdapter<K, V> {

		private final NotificationConsumer<K, V> consumer;

		public KeyNotificationListener(NotificationConsumer<K, V> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void message(K pattern, K channel, V message) {
			consumer.accept(channel, message);
		}

	}

	private static class ClusterKeyNotificationListener<K, V> extends RedisClusterPubSubAdapter<K, V> {

		private final NotificationConsumer<K, V> consumer;

		public ClusterKeyNotificationListener(NotificationConsumer<K, V> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void message(RedisClusterNode node, K pattern, K channel, V message) {
			consumer.accept(channel, message);
		}
	}

	@Override
	protected synchronized void doClose() throws Exception {
		if (publisher != null) {
			publisher.close();
			publisher = null;
		}
		if (queue != null) {
			if (!queue.isEmpty()) {
				log.warn("Queue still contains elements");
			}
			queue = null;
		}
		keySet = null;
	}

	@Override
	protected K doPoll(long timeout, TimeUnit unit) throws InterruptedException {
		K key = queue.poll(timeout, unit);
		if (key == null) {
			return null;
		}
		keySet.remove(new Wrapper<>(key));
		return key;
	}

	private DataType keyType(String event) {
		if (event == null) {
			return DataType.NONE;
		}
		String code = event.toLowerCase();
		if (code.startsWith("xgroup-")) {
			return DataType.STREAM;
		}
		if (code.startsWith("ts.")) {
			return DataType.TIMESERIES;
		}
		if (code.startsWith("json.")) {
			return DataType.JSON;
		}
		switch (code) {
		case "set":
		case "setrange":
		case "incrby":
		case "incrbyfloat":
		case "append":
			return DataType.STRING;
		case "lpush":
		case "rpush":
		case "rpop":
		case "lpop":
		case "linsert":
		case "lset":
		case "lrem":
		case "ltrim":
			return DataType.LIST;
		case "hset":
		case "hincrby":
		case "hincrbyfloat":
		case "hdel":
			return DataType.HASH;
		case "sadd":
		case "spop":
		case "sinterstore":
		case "sunionstore":
		case "sdiffstore":
			return DataType.SET;
		case "zincr":
		case "zadd":
		case "zrem":
		case "zrembyscore":
		case "zrembyrank":
		case "zdiffstore":
		case "zinterstore":
		case "zunionstore":
			return DataType.ZSET;
		case "xadd":
		case "xtrim":
		case "xdel":
		case "xsetid":
			return DataType.STREAM;
		default:
			return DataType.NONE;
		}
	}

	public int getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(int queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public void setKeyPattern(String keyPattern) {
		this.keyPattern = keyPattern;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	private static class RedisKeyNotificationPublisher<K, V> implements AutoCloseable {

		private final StatefulRedisPubSubConnection<K, V> connection;
		private final K pattern;
		private final RedisPubSubListener<K, V> listener;

		@SuppressWarnings("unchecked")
		public RedisKeyNotificationPublisher(RedisClient client, RedisCodec<K, V> codec,
				RedisPubSubListener<K, V> listener, K pattern) {
			this.connection = client.connectPubSub(codec);
			this.listener = listener;
			this.pattern = pattern;
			connection.addListener(listener);
			connection.sync().psubscribe(pattern);
		}

		@SuppressWarnings("unchecked")
		@Override
		public synchronized void close() {
			if (connection.isOpen()) {
				connection.sync().punsubscribe(pattern);
				connection.removeListener(listener);
				connection.close();
			}
		}

	}

	private static class RedisClusterKeyNotificationPublisher<K, V> implements AutoCloseable {

		private final StatefulRedisClusterPubSubConnection<K, V> connection;
		private final RedisClusterPubSubListener<K, V> listener;
		private final K pattern;

		@SuppressWarnings("unchecked")
		public RedisClusterKeyNotificationPublisher(RedisClusterClient client, RedisCodec<K, V> codec,
				RedisClusterPubSubListener<K, V> listener, K pattern) {
			this.connection = client.connectPubSub(codec);
			this.listener = listener;
			this.pattern = pattern;
			this.connection.setNodeMessagePropagation(true);
			this.connection.addListener(listener);
			this.connection.sync().upstream().commands().psubscribe(pattern);
		}

		@SuppressWarnings("unchecked")
		@Override
		public synchronized void close() throws Exception {
			if (connection.isOpen()) {
				connection.sync().upstream().commands().punsubscribe(pattern);
				connection.removeListener(listener);
				connection.close();
			}
		}

	}

}