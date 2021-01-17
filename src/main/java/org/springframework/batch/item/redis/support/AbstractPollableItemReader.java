package org.springframework.batch.item.redis.support;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements PollableItemReader<T> {

    private static final long DEFAULT_POLLING_TIMEOUT = 100;

    @Setter
    private long pollingTimeout = DEFAULT_POLLING_TIMEOUT;
    private boolean stopped;

    protected AbstractPollableItemReader() {
        setName(ClassUtils.getShortName(getClass()));
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

    protected boolean isTerminated() {
        return stopped;
    }


}
