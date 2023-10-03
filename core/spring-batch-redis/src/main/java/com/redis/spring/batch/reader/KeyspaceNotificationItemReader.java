package com.redis.spring.batch.reader;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.SetBlockingQueue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;
import io.micrometer.core.instrument.Metrics;

public class KeyspaceNotificationItemReader<K> extends AbstractItemStreamItemReader<K>
        implements KeyItemReader<K>, PollableItemReader<K> {

    public enum OrderingStrategy {
        FIFO, PRIORITY
    }

    public static final String MATCH_ALL = "*";

    public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    public static final OrderingStrategy DEFAULT_ORDERING = OrderingStrategy.PRIORITY;

    public static final String QUEUE_METER = "redis.batch.notification.queue.size";

    private static final KeyspaceNotificationComparator NOTIFICATION_COMPARATOR = new KeyspaceNotificationComparator();

    private final AbstractRedisClient client;

    private final Function<String, K> stringKeyEncoder;

    private int database;

    private String keyPattern;

    private OrderingStrategy orderingStrategy = DEFAULT_ORDERING;

    private DataType keyType;

    private int queueCapacity = RedisItemReader.DEFAULT_NOTIFICATION_QUEUE_CAPACITY;

    private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

    private BlockingQueue<KeyspaceNotification> queue;

    private ChannelMessagePublisher publisher;

    public KeyspaceNotificationItemReader(AbstractRedisClient client, RedisCodec<K, ?> codec) {
        this.client = client;
        this.stringKeyEncoder = CodecUtils.stringKeyFunction(codec);
    }

    public BlockingQueue<KeyspaceNotification> getQueue() {
        return queue;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setKeyPattern(String keyPattern) {
        this.keyPattern = keyPattern;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public void setKeyType(DataType keyType) {
        this.keyType = keyType;
    }

    public void setOrderingStrategy(OrderingStrategy orderingStrategy) {
        this.orderingStrategy = orderingStrategy;
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        if (publisher == null) {
            queue = new SetBlockingQueue<>(notificationQueue(), queueCapacity);
            Metrics.globalRegistry.gaugeCollectionSize(QUEUE_METER, Collections.emptyList(), queue);
            KeyspaceNotificationEnqueuer enqueuer = new KeyspaceNotificationEnqueuer(queue);
            enqueuer.setType(keyType);
            publisher = publisher(enqueuer);
        }
    }

    private ChannelMessagePublisher publisher(ChannelMessageConsumer consumer) {
        ChannelMessagePublisher messagePublisher = publisher();
        String pattern = String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern());
        messagePublisher.subscribe(consumer, pattern);
        messagePublisher.open();
        return messagePublisher;
    }

    private ChannelMessagePublisher publisher() {
        if (client instanceof RedisClusterClient) {
            return new RedisClusterChannelMessagePublisher((RedisClusterClient) client);
        }
        return new RedisChannelMessagePublisher((RedisClient) client);
    }

    private String keyPattern() {
        if (keyPattern == null) {
            return MATCH_ALL;
        }
        return keyPattern;
    }

    private BlockingQueue<KeyspaceNotification> notificationQueue() {
        if (orderingStrategy == OrderingStrategy.PRIORITY) {
            return new PriorityBlockingQueue<>(queueCapacity, NOTIFICATION_COMPARATOR);
        }
        return new LinkedBlockingQueue<>(queueCapacity);
    }

    @Override
    public boolean isOpen() {
        return publisher != null;
    }

    @Override
    public synchronized void close() {
        if (publisher != null) {
            publisher.close();
            publisher = null;
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

}
