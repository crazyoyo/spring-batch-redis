package org.springframework.batch.item.redis.support;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements PollableItemReader<T> {

    private final long pollingTimeout;
    private boolean stopped;

    protected AbstractPollableItemReader(Duration pollingTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(pollingTimeout, "Polling timeout is required.");
        this.pollingTimeout = pollingTimeout.toMillis();
    }

    @Override
    protected T doRead() throws Exception {
        T item;
        do {
            item = poll(pollingTimeout, TimeUnit.MILLISECONDS);
        } while (item == null && !isTerminated());
        return item;
    }

    public void stop() {
        this.stopped = true;
    }

    @Override
    public boolean isTerminated() {
        return stopped;
    }


}
