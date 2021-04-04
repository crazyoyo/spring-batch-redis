package org.springframework.batch.item.redis.support;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.function.Function;

@Slf4j
public class ScanKeyItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<K> {

    private final StatefulConnection<K, V> connection;
    private final Long count;
    private final String match;
    private final String type;
    private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
    private ScanIterator<K> iterator;
    private Long size;

    public ScanKeyItemReader(StatefulConnection<K, V> connection, Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync, Long count, String match, String type) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A Redis connection is required");
        Assert.notNull(sync, "A sync command function is required");
        this.connection = connection;
        this.sync = sync;
        this.count = count;
        this.match = match;
        this.type = type;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doOpen() {
        if (iterator != null) {
            return;
        }
        KeyScanArgs args = new KeyScanArgs();
        if (count != null) {
            args.limit(count);
        }
        if (match != null) {
            args.match(match);
        }
        if (type != null) {
            args.type(type);
        }
        this.iterator = ScanIterator.scan((RedisKeyCommands<K, V>) sync.apply(connection), args);
    }

    @Override
    protected synchronized void doClose() {
        iterator = null;
        size = null;
    }

    @Override
    protected synchronized K doRead() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }
}
