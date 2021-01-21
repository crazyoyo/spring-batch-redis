package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class ScanKeyItemReader<K, V> extends AbstractItemCountingItemStreamItemReader<K> implements BoundedItemReader<K> {

    private final StatefulConnection<K, V> connection;
    private final long scanCount;
    private final String scanMatch;
    private final Predicate<K> keyPatternPredicate;
    private final long commandTimeout;
    private final int sampleSize;
    private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async;
    private final Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync;
    private ScanIterator<K> iterator;
    private Long size;

    public ScanKeyItemReader(StatefulConnection<K, V> connection, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, Function<StatefulConnection<K, V>, BaseRedisCommands<K, V>> sync, Duration commandTimeout, long scanCount, String scanMatch, int sampleSize, Predicate<K> keyPatternPredicate) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A Redis connection is required");
        Assert.notNull(async, "An async command function is required");
        Assert.notNull(sync, "A sync command function is required");
        Assert.notNull(commandTimeout, "Command timeout is required");
        Assert.notNull(keyPatternPredicate, "A key predicate is required");
        this.connection = connection;
        this.async = async;
        this.sync = sync;
        this.commandTimeout = commandTimeout.getSeconds();
        this.scanCount = scanCount;
        this.scanMatch = scanMatch;
        this.sampleSize = sampleSize;
        this.keyPatternPredicate = keyPatternPredicate;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doOpen() throws InterruptedException, ExecutionException, TimeoutException {
        if (iterator != null) {
            return;
        }
        this.size = calculateSize();
        ScanArgs scanArgs = ScanArgs.Builder.limit(scanCount).match(scanMatch);
        this.iterator = ScanIterator.scan((RedisKeyCommands<K, V>) sync.apply(connection), scanArgs);
    }

    private Long calculateSize() throws InterruptedException, ExecutionException, TimeoutException {
        BaseRedisAsyncCommands<K, V> async = this.async.apply(connection);
        async.setAutoFlushCommands(false);
        RedisFuture<Long> dbsizeFuture = ((RedisServerAsyncCommands<K, V>) async).dbsize();
        List<RedisFuture<K>> keyFutures = new ArrayList<>(sampleSize);
        // rough estimate of keys matching pattern
        for (int index = 0; index < sampleSize; index++) {
            keyFutures.add(((RedisKeyAsyncCommands<K, V>) async).randomkey());
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        int matchCount = 0;
        for (RedisFuture<K> future : keyFutures) {
            K key = future.get(commandTimeout, TimeUnit.SECONDS);
            if (key == null) {
                continue;
            }
            if (keyPatternPredicate.test(key)) {
                matchCount++;
            }
        }
        Long dbsize = dbsizeFuture.get(commandTimeout, TimeUnit.SECONDS);
        if (dbsize == null) {
            return null;
        }
        return dbsize * matchCount / sampleSize;
    }

    @Override
    public Long size() {
        return size;
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
