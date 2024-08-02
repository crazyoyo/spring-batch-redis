package com.redis.spring.batch.item.redis.reader;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.util.ClassUtils;

import com.redis.spring.batch.item.AbstractPollableItemReader;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyEvent;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;

public class KeyNotificationItemReader<K, V> extends AbstractPollableItemReader<KeyEvent<K>> {

	public static final int DEFAULT_QUEUE_CAPACITY = 10000;

	private static final String KEYSPACE_PATTERN = "__keyspace@%s__:%s";
	private static final String KEYEVENT_PATTERN = "__keyevent@%s__:*";
	private static final String SEPARATOR = ":";

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final BiPredicate<K, K> keyEquals;
	private final Function<String, K> keyEncoder;
	private final Function<K, String> keyDecoder;
	private final Function<V, String> valueDecoder;
	private final Map<KeyEventStatus, AtomicLong> statusCounts = Stream.of(KeyEventStatus.values())
			.collect(Collectors.toMap(Function.identity(), s -> new AtomicLong()));

	private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
	private int database;
	private String keyPattern;
	private String keyType;
	private Set<KeyEventListener<K>> listeners = new LinkedHashSet<>();

	protected BlockingQueue<KeyEvent<K>> queue;
	private KeyNotificationPublisher publisher;

	public KeyNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.keyEquals = BatchUtils.keyEqualityPredicate(codec);
		this.keyEncoder = BatchUtils.stringKeyFunction(codec);
		this.keyDecoder = BatchUtils.toStringKeyFunction(codec);
		this.valueDecoder = BatchUtils.toStringValueFunction(codec);
	}

	public void addListener(KeyEventListener<K> listener) {
		this.listeners.add(listener);
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
	public boolean isComplete() {
		return publisher == null;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (queue == null) {
			queue = new LinkedBlockingQueue<>(queueCapacity);
		}
		if (publisher == null) {
			publisher = publisher();
			publisher.open();
		}
	}

	public boolean isOpen() {
		return publisher != null;
	}

	@Override
	protected synchronized void doClose() throws Exception {
		if (publisher != null) {
			publisher.close();
			publisher = null;
		}
		queue = null;
	}

	private void keySpaceNotification(K channel, V message) {
		K key = keyEncoder.apply(suffix(channel));
		String event = valueDecoder.apply(message);
		onKeyEvent(key, event);
	}

	@SuppressWarnings("unchecked")
	private void keyEventNotification(K channel, V message) {
		K key = (K) message;
		String event = suffix(channel);
		onKeyEvent(key, event);
	}

	private void onKeyEvent(K key, String event) {
		KeyEvent<K> keyEvent = KeyEvent.of(key, event);
		KeyEventStatus status = process(keyEvent);
		statusCounts.get(status).incrementAndGet();
		for (KeyEventListener<K> listener : listeners) {
			listener.event(keyEvent, status);
		}
	}

	private boolean accept(KeyEvent<K> event) {
		return keyType == null || keyType.equalsIgnoreCase(dataType(event).getString());
	}

	private KeyEventStatus process(KeyEvent<K> notification) {
		if (!accept(notification)) {
			return KeyEventStatus.REJECTED;
		}
		boolean removed = queue.removeIf(e -> keyEquals.test(e.getKey(), notification.getKey()));
		if (removed) {
			return KeyEventStatus.DEBOUNCED;
		}
		boolean added = queue.offer(notification);
		if (added) {
			return KeyEventStatus.ACCEPTED;
		}
		return KeyEventStatus.DROPPED;
	}

	public static DataType dataType(KeyEvent<?> event) {
		if (event == null) {
			return DataType.NONE;
		}
		String code = event.getEvent();
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

	private KeyNotificationConsumer<K, V> notificationConsumer() {
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

	private KeyNotificationPublisher publisher() {
		String pubSubPattern = pubSubPattern();
		K pattern = keyEncoder.apply(pubSubPattern);
		KeyNotificationConsumer<K, V> consumer = notificationConsumer();
		if (client instanceof RedisClusterClient) {
			RedisClusterPubSubListener<K, V> listener = new RedisClusterKeyNotificationListener<>(consumer);
			return new RedisClusterKeyNotificationPublisher<>((RedisClusterClient) client, codec, listener, pattern);
		}
		RedisPubSubListener<K, V> listener = new RedisKeyNotificationListener<>(consumer);
		return new RedisKeyNotificationPublisher<>((RedisClient) client, codec, listener, pattern);
	}

	@Override
	protected KeyEvent<K> doPoll(long timeout, TimeUnit unit) throws Exception {
		return queue.poll(timeout, unit);
	}

	public BlockingQueue<KeyEvent<K>> getQueue() {
		return queue;
	}

	public long count(KeyEventStatus status) {
		return statusCounts.get(status).get();
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

	public void setKeyPattern(String pattern) {
		this.keyPattern = pattern;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String type) {
		this.keyType = type;
	}

}