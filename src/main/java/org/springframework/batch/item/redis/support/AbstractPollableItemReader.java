package org.springframework.batch.item.redis.support;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPollableItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements PollableItemReader<T> {

    @Setter
    private long pollingTimeout = DEFAULT_POLLING_TIMEOUT;

    protected AbstractPollableItemReader() {
        setName(ClassUtils.getShortName(getClass()));
    }

    @Override
    protected T doRead() throws Exception {
        T item;
        do {
            item = poll(pollingTimeout, TimeUnit.MILLISECONDS);
        } while (item == null);
        return item;
    }

}
