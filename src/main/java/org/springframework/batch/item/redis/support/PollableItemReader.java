package org.springframework.batch.item.redis.support;

import org.springframework.batch.item.ItemReader;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public interface PollableItemReader<T> extends ItemReader<T> {

    long DEFAULT_POLLING_TIMEOUT = 100;
    Duration DEFAULT_POLLING_TIMEOUT_DURATION = Duration.ofMillis(DEFAULT_POLLING_TIMEOUT);

    /**
     * Tries to read a piece of input data. If such input is available within the given duration, advances to the next one otherwise
     * returns <code>null</code>.
     *
     * @return T the item to be processed or {@code null} if the specified waiting time elapses before an element is available
     */
    T poll(long timeout, TimeUnit unit) throws InterruptedException;

}
