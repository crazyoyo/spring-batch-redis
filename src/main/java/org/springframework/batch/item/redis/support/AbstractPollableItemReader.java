package org.springframework.batch.item.redis.support;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements PollableItemReader<T> {

    private final long timeout;
    private boolean running;

    protected AbstractPollableItemReader(Duration readTimeout) {
        Assert.notNull(readTimeout, "A read timeout is required");
        setName(ClassUtils.getShortName(getClass()));
        this.timeout = readTimeout.toMillis();
    }

    @Override
    protected void doOpen() throws Exception {
        this.running = true;
    }

    @Override
    protected T doRead() throws Exception {
        T item;
        do {
            item = poll(timeout, TimeUnit.MILLISECONDS);
        } while (item == null && isRunning());
        return item;
    }

    @Override
    protected void doClose() throws Exception {
        this.running = false;
    }

    protected boolean isRunning() {
        return running;
    }

    @Setter
    @Accessors(fluent = true)
    public static class PollableItemReaderBuilder<B extends PollableItemReaderBuilder<B>> {

        public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofMillis(100);

        protected Duration readTimeout = DEFAULT_READ_TIMEOUT;

        public B readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return (B) this;
        }
    }

}
