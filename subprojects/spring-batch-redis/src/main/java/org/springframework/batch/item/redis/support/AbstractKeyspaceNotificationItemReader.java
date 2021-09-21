package org.springframework.batch.item.redis.support;

import com.hybhub.util.concurrent.ConcurrentSetBlockingQueue;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
public abstract class AbstractKeyspaceNotificationItemReader<C extends StatefulRedisPubSubConnection<String, String>> extends ItemStreamSupport implements PollableItemReader<String> {

    private final Supplier<C> connectionSupplier;
    private final BlockingQueue<String> queue;
    private final List<String> pubSubPatterns;
    private C connection;

    protected AbstractKeyspaceNotificationItemReader(Supplier<C> connectionSupplier, List<String> pubSubPatterns, int queueCapacity) {
        this(connectionSupplier, pubSubPatterns, new ConcurrentSetBlockingQueue<>(queueCapacity));
    }

    protected AbstractKeyspaceNotificationItemReader(Supplier<C> connectionSupplier, List<String> pubSubPatterns, BlockingQueue<String> queue) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connectionSupplier, "A pub/sub connection supplier is required");
        Assert.notNull(queue, "A queue is required");
        Assert.notEmpty(pubSubPatterns, "A pub/sub pattern is required");
        this.connectionSupplier = connectionSupplier;
        this.queue = queue;
        this.pubSubPatterns = pubSubPatterns;
    }

    @Override
    public String read() throws Exception {
        throw new IllegalAccessException("read() method should not be called");
    }

    @Override
    public synchronized void open(ExecutionContext executionContext) throws ItemStreamException {
        if (connection == null) {
            MetricsUtils.createGaugeCollectionSize("reader.notification.queue.size", queue);
            log.debug("Connecting to Redis pub/sub");
            this.connection = connectionSupplier.get();
            subscribe(connection, pubSubPatterns);
        }
    }

    protected abstract void subscribe(C connection, List<String> patterns);

    @Override
    public String poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    public synchronized void close() throws ItemStreamException {
        if (connection == null) {
            return;
        }
        unsubscribe(connection, pubSubPatterns);
        connection.close();
        connection = null;
    }

    protected abstract void unsubscribe(C connection, List<String> patterns);

    protected void add(String message) {
        if (message == null) {
            return;
        }
        String key = message.substring(message.indexOf(":") + 1);
        if (key == null) {
            return;
        }
        if (queue.offer(key)) {
            log.debug("Adding key '{}' to notification queue", key);
        } else {
            log.debug("Could not add key '{}': notification queue full", key);
        }
    }

}
