package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Map;
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
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.util.Helper;
import com.redis.spring.batch.util.SetBlockingQueue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubListener;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class KeyspaceNotificationItemReader<K, V> extends AbstractItemStreamItemReader<K>
        implements PollableItemReader<K>, RedisPubSubListener<String, String>, RedisClusterPubSubListener<String, String> {

    public enum OrderingStrategy {
        FIFO, PRIORITY
    }

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    public static final OrderingStrategy DEFAULT_ORDERING = OrderingStrategy.PRIORITY;

    public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";

    private static final String SEPARATOR = ":";

    private static final Map<String, KeyEvent> keyEvents = Stream.of(KeyEvent.values())
            .collect(Collectors.toMap(KeyEvent::getString, Function.identity()));

    private static final KeyspaceNotificationComparator NOTIFICATION_COMPARATOR = new KeyspaceNotificationComparator();

    private final Log logger = LogFactory.getLog(getClass());

    private final AbstractRedisClient client;

    private final Function<String, K> stringKeyEncoder;

    protected String pattern = RedisItemReader.DEFAULT_PUBSUB_PATTERN;

    private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

    private String keyType;

    private int queueCapacity = RedisItemReader.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private BlockingQueue<KeyspaceNotification> queue;

    private StatefulRedisPubSubConnection<String, String> connection;

    public KeyspaceNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.client = client;
        this.stringKeyEncoder = Helper.stringKeyFunction(codec);
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public OrderingStrategy getOrderingStrategy() {
        return orderingStrategy;
    }

    public void setOrderingStrategy(OrderingStrategy orderingStrategy) {
        this.orderingStrategy = orderingStrategy;
    }

    public BlockingQueue<KeyspaceNotification> getQueue() {
        return queue;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        if (!isOpen()) {
            queue = new SetBlockingQueue<>(notificationQueue());
            Helper.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
            connection = RedisModulesUtils.pubSubConnection(client);
            if (connection instanceof StatefulRedisClusterPubSubConnection) {
                StatefulRedisClusterPubSubConnection<String, String> clusterConnection = (StatefulRedisClusterPubSubConnection<String, String>) connection;
                clusterConnection.addListener((RedisClusterPubSubListener<String, String>) this);
                clusterConnection.setNodeMessagePropagation(true);
                clusterConnection.sync().upstream().commands().psubscribe(pattern);
            } else {
                connection.sync().psubscribe(pattern);
                connection.addListener(this);
            }

        }
    }

    private BlockingQueue<KeyspaceNotification> notificationQueue() {
        if (orderingStrategy == OrderingStrategy.PRIORITY) {
            return new PriorityBlockingQueue<>(queueCapacity, NOTIFICATION_COMPARATOR);
        }
        return new LinkedBlockingQueue<>(queueCapacity);
    }

    public boolean isOpen() {
        return connection != null;
    }

    protected void notification(String channel, String message) {
        if (channel == null) {
            return;
        }
        String key = channel.substring(channel.indexOf(SEPARATOR) + 1);
        KeyEvent event = keyEvent(message);
        if (keyType == null || keyType.equalsIgnoreCase(event.getType())) {
            KeyspaceNotification notification = new KeyspaceNotification();
            notification.setKey(key);
            notification.setEvent(event);
            boolean added = queue.offer(notification);
            if (!added) {
                // could not add notification because the queue is full
                logger.warn("Keyspace notification queue is full");
            }
        }
    }

    private KeyEvent keyEvent(String event) {
        return keyEvents.getOrDefault(event, KeyEvent.UNKNOWN);
    }

    @Override
    public synchronized void close() {
        if (isOpen()) {
            if (connection instanceof StatefulRedisClusterPubSubConnection) {
                StatefulRedisClusterPubSubConnection<String, String> clusterConnection = (StatefulRedisClusterPubSubConnection<String, String>) connection;
                clusterConnection.sync().upstream().commands().punsubscribe(pattern);
                clusterConnection.removeListener((RedisClusterPubSubListener<String, String>) this);
            } else {
                connection.sync().punsubscribe(pattern);
                connection.removeListener(this);
            }
            connection.close();
            connection = null;
        }
        super.close();
    }

    @Override
    public K read() throws Exception {
        return poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public K poll(long timeout, TimeUnit unit) throws InterruptedException {
        KeyspaceNotification notification = queue.poll(timeout, unit);
        if (notification == null) {
            return null;
        }
        return stringKeyEncoder.apply(notification.getKey());
    }

    @Override
    public void message(String channel, String message) {
        notification(channel, message);
    }

    @Override
    public void message(String pattern, String channel, String message) {
        notification(channel, message);
    }

    @Override
    public void subscribed(String channel, long count) {
        // Do nothing
    }

    @Override
    public void psubscribed(String pattern, long count) {
        // Do nothing
    }

    @Override
    public void unsubscribed(String channel, long count) {
        // Do nothing
    }

    @Override
    public void punsubscribed(String pattern, long count) {
        // Do nothing
    }

    @Override
    public void message(RedisClusterNode node, String channel, String message) {
        notification(channel, message);
    }

    @Override
    public void message(RedisClusterNode node, String pattern, String channel, String message) {
        notification(channel, message);
    }

    @Override
    public void subscribed(RedisClusterNode node, String channel, long count) {
        // Do nothing
    }

    @Override
    public void psubscribed(RedisClusterNode node, String pattern, long count) {
        // Do nothing
    }

    @Override
    public void unsubscribed(RedisClusterNode node, String channel, long count) {
        // Do nothing
    }

    @Override
    public void punsubscribed(RedisClusterNode node, String pattern, long count) {
        // Do nothing
    }

}
