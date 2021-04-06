package org.springframework.batch.item.redis.support;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPollableItemReader<T> extends ItemStreamSupport implements PollableItemReader<T>{

    private final long timeout;
    private boolean running;

    protected AbstractPollableItemReader(Duration readTimeout) {
        Assert.notNull(readTimeout, "A read timeout is required");
        setName(ClassUtils.getShortName(getClass()));
        this.timeout = readTimeout.toMillis();
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.running = true;
    }

    @Override
    public void close() {
        this.running = false;
    }

    @Override
    public T read() throws Exception {
        T item;
        do {
            item = poll(timeout, TimeUnit.MILLISECONDS);
        } while (item == null && isRunning());
        return item;
    }


    protected boolean isRunning() {
        return running;
    }

    @SuppressWarnings("unchecked")
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
