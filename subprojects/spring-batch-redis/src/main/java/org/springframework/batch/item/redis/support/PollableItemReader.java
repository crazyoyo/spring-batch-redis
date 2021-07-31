package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemStreamReader;

import java.util.concurrent.TimeUnit;

public interface PollableItemReader<T> extends ItemStreamReader<T> {

    /**
     * Tries to read a piece of input data. If such input is available within the given duration, advances to the next one otherwise
     * returns <code>null</code>.
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
     * @return T the item to be processed or {@code null} if the specified waiting time elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     */
    T poll(long timeout, TimeUnit unit) throws Exception;

}
