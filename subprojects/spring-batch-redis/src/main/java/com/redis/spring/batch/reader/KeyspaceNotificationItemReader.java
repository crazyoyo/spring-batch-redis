package com.redis.spring.batch.reader;

import static com.redis.spring.batch.reader.KeyEventType.APPEND;
import static com.redis.spring.batch.reader.KeyEventType.COPY_TO;
import static com.redis.spring.batch.reader.KeyEventType.DEL;
import static com.redis.spring.batch.reader.KeyEventType.EVICTED;
import static com.redis.spring.batch.reader.KeyEventType.EXPIRE;
import static com.redis.spring.batch.reader.KeyEventType.EXPIRED;
import static com.redis.spring.batch.reader.KeyEventType.HDEL;
import static com.redis.spring.batch.reader.KeyEventType.HINCRBY;
import static com.redis.spring.batch.reader.KeyEventType.HINCRBYFLOAT;
import static com.redis.spring.batch.reader.KeyEventType.HSET;
import static com.redis.spring.batch.reader.KeyEventType.INCRBY;
import static com.redis.spring.batch.reader.KeyEventType.INCRBYFLOAT;
import static com.redis.spring.batch.reader.KeyEventType.JSON_SET;
import static com.redis.spring.batch.reader.KeyEventType.LINSERT;
import static com.redis.spring.batch.reader.KeyEventType.LPOP;
import static com.redis.spring.batch.reader.KeyEventType.LPUSH;
import static com.redis.spring.batch.reader.KeyEventType.LREM;
import static com.redis.spring.batch.reader.KeyEventType.LSET;
import static com.redis.spring.batch.reader.KeyEventType.LTRIM;
import static com.redis.spring.batch.reader.KeyEventType.MOVE_FROM;
import static com.redis.spring.batch.reader.KeyEventType.MOVE_TO;
import static com.redis.spring.batch.reader.KeyEventType.NEW_KEY;
import static com.redis.spring.batch.reader.KeyEventType.PERSIST;
import static com.redis.spring.batch.reader.KeyEventType.RENAME_FROM;
import static com.redis.spring.batch.reader.KeyEventType.RENAME_TO;
import static com.redis.spring.batch.reader.KeyEventType.RESTORE;
import static com.redis.spring.batch.reader.KeyEventType.RPOP;
import static com.redis.spring.batch.reader.KeyEventType.RPUSH;
import static com.redis.spring.batch.reader.KeyEventType.SADD;
import static com.redis.spring.batch.reader.KeyEventType.SDIFFSTORE;
import static com.redis.spring.batch.reader.KeyEventType.SET;
import static com.redis.spring.batch.reader.KeyEventType.SETRANGE;
import static com.redis.spring.batch.reader.KeyEventType.SINTERSTORE;
import static com.redis.spring.batch.reader.KeyEventType.SORTSTORE;
import static com.redis.spring.batch.reader.KeyEventType.SPOP;
import static com.redis.spring.batch.reader.KeyEventType.SUNIONSTORE;
import static com.redis.spring.batch.reader.KeyEventType.TS_ADD;
import static com.redis.spring.batch.reader.KeyEventType.UNKNOWN;
import static com.redis.spring.batch.reader.KeyEventType.XADD;
import static com.redis.spring.batch.reader.KeyEventType.XDEL;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_CREATE;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_CREATECONSUMER;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_DELCONSUMER;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_DESTROY;
import static com.redis.spring.batch.reader.KeyEventType.XGROUP_SETID;
import static com.redis.spring.batch.reader.KeyEventType.XSETID;
import static com.redis.spring.batch.reader.KeyEventType.XTRIM;
import static com.redis.spring.batch.reader.KeyEventType.ZADD;
import static com.redis.spring.batch.reader.KeyEventType.ZDIFFSTORE;
import static com.redis.spring.batch.reader.KeyEventType.ZINCR;
import static com.redis.spring.batch.reader.KeyEventType.ZINTERSTORE;
import static com.redis.spring.batch.reader.KeyEventType.ZREM;
import static com.redis.spring.batch.reader.KeyEventType.ZREMBYRANK;
import static com.redis.spring.batch.reader.KeyEventType.ZREMBYSCORE;
import static com.redis.spring.batch.reader.KeyEventType.ZUNIONSTORE;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.SetBlockingQueue;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class KeyspaceNotificationItemReader extends AbstractItemCountingItemStreamItemReader<String>
		implements PollableItemReader<String>, AutoCloseable {

	protected static final KeyEventType[] EVENT_TYPES_ORDERED = {
			// Delete
			DEL, EXPIRED, EVICTED, EXPIRE,
			// Create
			PERSIST, NEW_KEY, RESTORE, RENAME_FROM, RENAME_TO, MOVE_FROM, MOVE_TO, COPY_TO,
			// String
			SET, SETRANGE, INCRBY, INCRBYFLOAT, APPEND,
			// Hash
			HSET, HINCRBY, HINCRBYFLOAT, HDEL,
			// JSON
			JSON_SET,			
			// List
			LPUSH, RPUSH, RPOP, LPOP, LINSERT, LSET, LREM, LTRIM, SORTSTORE,
			// Set
			SADD, SPOP, SINTERSTORE, SUNIONSTORE, SDIFFSTORE,
			// Sorted Set
			ZINCR, ZADD, ZREM, ZREMBYSCORE, ZREMBYRANK, ZDIFFSTORE, ZINTERSTORE, ZUNIONSTORE,
			// Stream
			XADD, XTRIM, XDEL, XGROUP_CREATE, XGROUP_CREATECONSUMER, XGROUP_DELCONSUMER, XGROUP_DESTROY, XGROUP_SETID,
			XSETID,
			// TimeSeries
			TS_ADD,
			// Other
			UNKNOWN };

	private static final Log log = LogFactory.getLog(KeyspaceNotificationItemReader.class);
	private static final String SEPARATOR = ":";
	public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";

	private KeyspaceNotificationReaderOptions options = KeyspaceNotificationReaderOptions.builder().build();
	private final AbstractRedisClient client;
	private final Map<String, KeyEventType> eventTypes = Stream.of(KeyEventType.values())
			.collect(Collectors.toMap(KeyEventType::getString, Function.identity()));
	private Publisher publisher;
	private BlockingQueue<KeyspaceNotification> queue;

	public KeyspaceNotificationItemReader(AbstractRedisClient client) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
	}

	public BlockingQueue<KeyspaceNotification> getQueue() {
		return queue;
	}

	public KeyspaceNotificationReaderOptions getOptions() {
		return options;
	}

	public void setOptions(KeyspaceNotificationReaderOptions options) {
		this.options = options;
	}

	@Override
	protected synchronized void doOpen() {
		if (publisher != null) {
			return;
		}
		queue = new SetBlockingQueue<>(new PriorityBlockingQueue<>(options.getQueueOptions().getCapacity(),
				new KeyspaceNotificationComparator()));
		Utils.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
		publisher = publisher();
		log.debug("Subscribing to keyspace notifications");
		publisher.open(options.getPatterns().toArray(new String[0]));
	}

	private static class KeyspaceNotificationComparator implements Comparator<KeyspaceNotification> {

		private final Map<KeyEventType, Integer> ranking;

		public KeyspaceNotificationComparator() {
			this.ranking = new EnumMap<>(KeyEventType.class);
			for (int index = 0; index < EVENT_TYPES_ORDERED.length; index++) {
				ranking.put(EVENT_TYPES_ORDERED[index], index);
			}
		}

		@Override
		public int compare(KeyspaceNotification o1, KeyspaceNotification o2) {
			return ranking.getOrDefault(o1.getEventType(), Integer.MAX_VALUE)
					- ranking.getOrDefault(o2.getEventType(), Integer.MAX_VALUE);
		}

	}

	private Publisher publisher() {
		StatefulRedisPubSubConnection<String, String> connection = RedisModulesUtils.pubSubConnection(client);
		if (connection instanceof StatefulRedisClusterPubSubConnection) {
			return new RedisClusterPublisher((StatefulRedisClusterPubSubConnection<String, String>) connection);
		}
		return new RedisPublisher(connection);
	}

	@Override
	protected synchronized void doClose() {
		if (publisher == null) {
			return;
		}
		log.debug("Unsubscribing from keyspace notifications");
		publisher.close(options.getPatterns().toArray(new String[0]));
		publisher = null;
	}

	@Override
	public boolean isOpen() {
		return publisher != null;
	}

	private void notification(String channel, String message) {
		if (channel == null) {
			return;
		}
		String key = channel.substring(channel.indexOf(SEPARATOR) + 1);
		KeyspaceNotification notification = new KeyspaceNotification(key, eventType(message));
		if (!queue.offer(notification)) {
			log.warn("Could not add key because queue is full");
		}
	}

	private KeyEventType eventType(String message) {
		if (!eventTypes.containsKey(message)) {
			System.out.println("Unknown event type: " + message);
		}
		return eventTypes.getOrDefault(message, KeyEventType.UNKNOWN);
	}

	@Override
	protected String doRead() throws Exception {
		return poll(options.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public String poll(long timeout, TimeUnit unit) throws InterruptedException {
		KeyspaceNotification notification = queue.poll(timeout, unit);
		if (notification == null) {
			return null;
		}
		return notification.getKey();
	}

	private interface Publisher {

		void open(String... patterns);

		void close(String... patterns);
	}

	private class RedisPublisher extends RedisPubSubAdapter<String, String> implements Publisher {

		private final StatefulRedisPubSubConnection<String, String> connection;

		public RedisPublisher(StatefulRedisPubSubConnection<String, String> connection) {
			this.connection = connection;
		}

		@Override
		public void open(String... patterns) {
			connection.addListener(this);
			connection.sync().psubscribe(patterns);
		}

		@Override
		public void close(String... patterns) {
			connection.sync().punsubscribe(patterns);
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
		public void open(String... patterns) {
			connection.addListener(this);
			connection.setNodeMessagePropagation(true);
			connection.sync().upstream().commands().psubscribe(patterns);
		}

		@Override
		public void close(String... patterns) {
			connection.sync().upstream().commands().punsubscribe(patterns);
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
