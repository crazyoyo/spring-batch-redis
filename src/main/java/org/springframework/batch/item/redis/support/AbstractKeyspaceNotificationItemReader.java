package org.springframework.batch.item.redis.support;

import com.hybhub.util.concurrent.ConcurrentSetBlockingQueue;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.*;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ClassUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
public abstract class AbstractKeyspaceNotificationItemReader<K, V, C extends StatefulRedisPubSubConnection<K, V>> extends ItemStreamSupport implements PollableItemReader<K> {

    private final Supplier<C> connectionSupplier;
    private final K pattern;
    private final Converter<K, K> keyExtractor;
    private final BlockingQueue<K> queue;
    private C connection;

    protected AbstractKeyspaceNotificationItemReader(Supplier<C> connectionSupplier, K pattern, Converter<K, K> keyExtractor, BlockingQueue<K> queue) {
        setName(ClassUtils.getShortName(getClass()));
        this.connectionSupplier = connectionSupplier;
        this.pattern = pattern;
        this.keyExtractor = keyExtractor;
        this.queue = queue;
    }

    @Override
    public K read() throws Exception {
        throw new IllegalAccessException("read() method should not be called");
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        if (connection == null) {
            MetricsUtils.createGaugeCollectionSize("reader.notification.queue.size", queue);
            log.debug("Connecting to Redis pub/sub");
            this.connection = connectionSupplier.get();
            subscribe(connection, pattern);
        }
    }

    protected abstract void subscribe(C connection, K pattern);

    @Override
    public K poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    public synchronized void close() throws ItemStreamException {
        if (connection == null) {
            return;
        }
        unsubscribe(connection, pattern);
        connection.close();
        connection = null;
    }

    protected abstract void unsubscribe(C connection, K pattern);

    protected void add(K message) {
        if (message == null) {
            return;
        }
        K key = keyExtractor.convert(message);
        if (key == null) {
            return;
        }
        boolean success = queue.offer(key);
        if (!success) {
            log.debug("Notification queue full for key '{}' (size={})", key, queue.size());
        }
    }

    @SuppressWarnings("unchecked")
    public static class KeyspaceNotificationItemReaderBuilder<B extends KeyspaceNotificationItemReaderBuilder<B>> {

        public static final int DEFAULT_DATABASE = 0;
        public static final int DEFAULT_QUEUE_CAPACITY = 1000;
        public static final String DEFAULT_KEY_PATTERN = "*";
        protected static final Converter<String, String> STRING_KEY_EXTRACTOR = m -> m.substring(m.indexOf(":") + 1);
        private static final String PUBSUB_PATTERN_FORMAT = "__keyspace@%s__:%s";

        private String keyPattern = DEFAULT_KEY_PATTERN;
        private int database = DEFAULT_DATABASE;
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;

        public B queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return (B) this;
        }

        public B keyPattern(String keyPattern) {
            this.keyPattern = keyPattern;
            return (B) this;
        }

        public B database(int database) {
            this.database = database;
            return (B) this;
        }

        protected String pattern() {
            return String.format(PUBSUB_PATTERN_FORMAT, database, keyPattern);
        }

        protected BlockingQueue<String> queue() {
            return new ConcurrentSetBlockingQueue<>(queueCapacity);
        }

    }
}
