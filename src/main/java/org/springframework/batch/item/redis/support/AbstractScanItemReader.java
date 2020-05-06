package org.springframework.batch.item.redis.support;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import lombok.Getter;
import lombok.Setter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Iterator;

public abstract class AbstractScanItemReader<K> extends AbstractItemCountingItemStreamItemReader<K> {

    private final Object lock = new Object();
    @Getter
    @Setter
    private Long count;
    @Getter
    @Setter
    private String match;
    private ScanArgs args = new ScanArgs();
    private Iterator<K> keys;
    private KeyScanCursor<K> cursor;

    protected AbstractScanItemReader(Long count, String match) {
        setName(ClassUtils.getShortName(getClass()));
        this.count = count;
        this.match = match;
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.isNull(cursor, "Iterator already started");
        args = new ScanArgs();
        if (count != null) {
            args.limit(count);
        }
        if (match != null) {
            args.match(match);
        }
        cursor = scan(args);
        keys = cursor.getKeys().iterator();
    }

    protected abstract KeyScanCursor<K> scan(ScanArgs args);

    @Override
    protected void doClose() {
        cursor = null;
        keys = null;
    }

    @Override
    protected K doRead() throws Exception {
        synchronized (lock) {
            if (keys.hasNext()) {
                return keys.next();
            }
            if (cursor.isFinished()) {
                return null;
            }
            do {
                cursor = scan(cursor, args);
                keys = cursor.getKeys().iterator();
            } while (!keys.hasNext() && !cursor.isFinished());
            if (keys.hasNext()) {
                return keys.next();
            }
            return null;
        }
    }

    public boolean isDone() {
        if (keys == null || cursor == null) {
            return false;
        }
        return !keys.hasNext() && cursor.isFinished();
    }

    protected abstract KeyScanCursor<K> scan(KeyScanCursor<K> cursor, ScanArgs args);

}
