package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;

@Slf4j
public abstract class AbstractKeyItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractItemCountingItemStreamItemReader<K> {

    private final C connection;
    private final long scanCount;
    private final String scanMatch;
    private ScanIterator<K> iterator;

    public AbstractKeyItemReader(C connection, Duration commandTimeout, long scanCount, String scanMatch) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A Redis connection is required.");
        Assert.notNull(commandTimeout, "Command timeout is required.");
        this.connection = connection;
        this.scanCount = scanCount;
        this.scanMatch = scanMatch;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doOpen() {
        if (iterator != null) {
            return;
        }
        ScanArgs scanArgs = ScanArgs.Builder.limit(scanCount).match(scanMatch);
        this.iterator = ScanIterator.scan((RedisKeyCommands<K, V>) sync(connection), scanArgs);
    }

    protected abstract BaseRedisAsyncCommands<K, V> async(C connection);

    protected abstract BaseRedisCommands<K, V> sync(C connection);

    @Override
    protected synchronized void doClose() {
        iterator = null;
    }

    @Override
    protected synchronized K doRead() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

}
