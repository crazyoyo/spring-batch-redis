package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;

public abstract class AbstractPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements PollableItemReader<T> {

    private final Duration pollingTimeout;

    protected AbstractPollableItemReader(Duration pollingTimeout) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(pollingTimeout, "Polling timeout is required.");
        this.pollingTimeout = pollingTimeout;
    }

    @Override
    protected T doRead() throws Exception {
        T item;
        do {
            item = poll(pollingTimeout);
        } while (item == null && !isTerminated());
        return item;
    }

}
