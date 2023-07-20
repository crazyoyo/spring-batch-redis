package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;

import com.redis.spring.batch.common.SetBlockingQueue;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyspaceNotificationItemReader<K, V> extends AbstractItemStreamItemReader<K>
        implements KeyItemReader<K>, PollableItemReader<K> {

    public static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

    public static final String QUEUE_SIZE_GAUGE_NAME = "reader.notification.queue.size";

    private static final String SEPARATOR = ":";

    private static final KeyspaceNotificationComparator NOTIFICATION_COMPARATOR = new KeyspaceNotificationComparator();

    private static final Map<String, KeyEvent> keyEvents = Stream.of(KeyEvent.values())
            .collect(Collectors.toMap(KeyEvent::getString, Function.identity()));

    private final AbstractRedisClient client;

    private final Function<String, K> keyDecoder;

    private final List<KeyspaceNotificationListener> listeners = new ArrayList<>();

    private ScanOptions scanOptions = ScanOptions.builder().build();

    private KeyspaceNotificationOptions keyspaceNotificationOptions = KeyspaceNotificationOptions.builder().build();

    private BlockingQueue<KeyspaceNotification> queue;

    private ItemStream publisher;

    // Used to dynamically block problematic keys (e.g. big keys)
    private final Set<String> blockedKeys = new HashSet<>();

    public KeyspaceNotificationItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        this.client = client;
        this.keyDecoder = Utils.stringKeyFunction(codec);
    }

    public Set<String> getBlockedKeys() {
        return blockedKeys;
    }

    public void blockKeys(String... keys) {
        blockKeys(Arrays.asList(keys));
    }

    public void blockKeys(Iterable<String> keys) {
        keys.forEach(blockedKeys::add);
    }

    public void addListener(KeyspaceNotificationListener listener) {
        this.listeners.add(listener);
    }

    public ScanOptions getScanOptions() {
        return scanOptions;
    }

    public void setScanOptions(ScanOptions scanOptions) {
        this.scanOptions = scanOptions;
    }

    public KeyspaceNotificationOptions getKeyspaceNotificationOptions() {
        return keyspaceNotificationOptions;
    }

    public void setKeyspaceNotificationOptions(KeyspaceNotificationOptions options) {
        this.keyspaceNotificationOptions = options;
    }

    public BlockingQueue<KeyspaceNotification> getQueue() {
        return queue;
    }

    private String pattern(int database, String match) {
        return String.format(PUBSUB_PATTERN_FORMAT, database, match);
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        if (!isOpen()) {
            queue = new SetBlockingQueue<>(notificationQueue());
            Utils.createGaugeCollectionSize(QUEUE_SIZE_GAUGE_NAME, queue);
            publisher = publisher();
            publisher.open(executionContext);
        }
    }

    public boolean isOpen() {
        return publisher != null;
    }

    private BlockingQueue<KeyspaceNotification> notificationQueue() {
        if (keyspaceNotificationOptions.getOrderingStrategy() == KeyspaceNotificationOrderingStrategy.PRIORITY) {
            return new PriorityBlockingQueue<>(keyspaceNotificationOptions.getQueueOptions().getCapacity(),
                    NOTIFICATION_COMPARATOR);
        }
        return new LinkedBlockingQueue<>(keyspaceNotificationOptions.getQueueOptions().getCapacity());
    }

    private ItemStream publisher() {
        String pattern = pattern(keyspaceNotificationOptions.getDatabase(), scanOptions.getMatch());
        BiConsumer<String, String> consumer = this::notification;
        if (client instanceof RedisClusterClient) {
            return new RedisClusterKeyspaceNotificationPublisher((RedisClusterClient) client, pattern, consumer);
        }
        return new RedisKeyspaceNotificationPublisher((RedisClient) client, pattern, consumer);
    }

    private void notification(String channel, String message) {
        String key = key(channel);
        if (isBlocked(key)) {
            return;
        }
        KeyEvent event = keyEvent(message);
        if (!accept(event)) {
            return;
        }
        KeyspaceNotification notification = new KeyspaceNotification();
        notification.setKey(key);
        notification.setEvent(event);
        boolean added = queue.offer(notification);
        if (!added) {
            // could not add notification because the queue is full. Notify listeners.
            listeners.forEach(l -> l.queueFull(notification));
        }
    }

    private boolean isBlocked(String key) {
        return blockedKeys.contains(key);
    }

    private boolean accept(KeyEvent event) {
        Optional<String> type = scanOptions.getType();
        if (type.isPresent()) {
            return type.get().equals(event.getType());
        }
        return true;
    }

    private KeyEvent keyEvent(String event) {
        return keyEvents.getOrDefault(event, KeyEvent.UNKNOWN);
    }

    @Override
    public synchronized void close() {
        if (isOpen()) {
            publisher.close();
            publisher = null;
        }
        super.close();
    }

    @Override
    public K read() throws Exception {
        return poll(keyspaceNotificationOptions.getQueueOptions().getPollTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public K poll(long timeout, TimeUnit unit) throws InterruptedException {
        KeyspaceNotification notification = queue.poll(timeout, unit);
        if (notification == null) {
            return null;
        }
        return keyDecoder.apply(notification.getKey());
    }

    private static String key(String channel) {
        if (channel == null) {
            return null;
        }
        return channel.substring(channel.indexOf(SEPARATOR) + 1);
    }

    public static class Builder<K, V> {

        private final AbstractRedisClient client;

        private final RedisCodec<K, V> codec;

        private ScanOptions scanOptions = ScanOptions.builder().build();

        private KeyspaceNotificationOptions keyspaceNotificationOptions = KeyspaceNotificationOptions.builder().build();

        public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
            this.client = client;
            this.codec = codec;
        }

        public Builder<K, V> scanOptions(ScanOptions options) {
            this.scanOptions = options;
            return this;
        }

        public Builder<K, V> keyspaceNotificationOptions(KeyspaceNotificationOptions options) {
            this.keyspaceNotificationOptions = options;
            return this;
        }

        public KeyspaceNotificationItemReader<K, V> build() {
            KeyspaceNotificationItemReader<K, V> reader = new KeyspaceNotificationItemReader<>(client, codec);
            reader.setScanOptions(scanOptions);
            reader.setKeyspaceNotificationOptions(keyspaceNotificationOptions);
            return reader;
        }

    }

}
