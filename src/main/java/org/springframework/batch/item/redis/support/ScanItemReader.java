package org.springframework.batch.item.redis.support;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisKeyCommands;
import lombok.Builder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;
import java.util.function.Function;

public class ScanItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractItemCountingItemStreamItemReader<K> {

    private final C connection;
    private final Function<C, RedisKeyCommands<K, V>> commands;
    private final ScanArgs scanArgs;

    private final Object lock = new Object();
    private Iterator<K> keyIterator;
    private KeyScanCursor<K> cursor;

    @Builder
    public ScanItemReader(C connection, Function<C, RedisKeyCommands<K, V>> commands, ScanArgs scanArgs) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A connection is required.");
        Assert.notNull(commands, "A commands function is required.");
        Assert.notNull(scanArgs, "Scan args are required.");
        this.connection = connection;
        this.commands = commands;
        this.scanArgs = scanArgs;
    }

    @Override
    protected void doOpen() {
        Assert.isNull(cursor, "Iterator already started");
        cursor = commands.apply(connection).scan(scanArgs);
        keyIterator = cursor.getKeys().iterator();
    }

    @Override
    protected void doClose() {
        cursor = null;
        keyIterator = null;
    }

    @Override
    protected K doRead() {
        synchronized (lock) {
            if (keyIterator.hasNext()) {
                return keyIterator.next();
            }
            if (cursor.isFinished()) {
                return null;
            }
            do {
                cursor = commands.apply(connection).scan(cursor, scanArgs);
                keyIterator = cursor.getKeys().iterator();
            } while (!keyIterator.hasNext() && !cursor.isFinished());
            if (keyIterator.hasNext()) {
                return keyIterator.next();
            }
            return null;
        }
    }

    public boolean isDone() {
        if (keyIterator == null || cursor == null) {
            return false;
        }
        return !keyIterator.hasNext() && cursor.isFinished();
    }

}
