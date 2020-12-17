package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemReader;

import java.time.Duration;

public interface PollableItemReader<T> extends ItemReader<T> {

    /**
     * Tries to read a piece of input data. If such input is available within the given duration, advances to the next one otherwise
     * returns <code>null</code>.
     *
     * @return T the item to be processed or {@code null} if the specified waiting time elapses before an element is available
     */
    T poll(Duration timeout) throws InterruptedException;

    boolean isTerminated();
}
