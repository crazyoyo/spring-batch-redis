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
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Slf4j
public abstract class AbstractKeyItemReader<K, V, C extends StatefulConnection<K, V>> extends AbstractProgressReportingItemReader<K> {

    private final C connection;
    private final long scanCount;
    private final String scanMatch;
    private final int sampleSize;
    private final long commandTimeout;
    private ScanIterator<K> iterator;

    public AbstractKeyItemReader(C connection, Duration commandTimeout, long scanCount, String scanMatch, int sampleSize) {
        setName(ClassUtils.getShortName(getClass()));
        Assert.notNull(connection, "A Redis connection is required.");
        Assert.notNull(commandTimeout, "Command timeout is required.");
        this.connection = connection;
        this.commandTimeout = commandTimeout.getSeconds();
        this.scanCount = scanCount;
        this.scanMatch = scanMatch;
        this.sampleSize = sampleSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void doOpen() throws Exception {
        if (iterator != null) {
            return;
        }
        BaseRedisAsyncCommands<K, V> async = async(connection);
        async.setAutoFlushCommands(false);
        RedisFuture<Long> dbsizeFuture = ((RedisServerAsyncCommands<String, String>) async).dbsize();
        List<RedisFuture<String>> keyFutures = new ArrayList<>(sampleSize);
        // rough estimate of keys matching pattern
        for (int index = 0; index < sampleSize; index++) {
            keyFutures.add(((RedisKeyAsyncCommands<String, String>) async).randomkey());
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        int matchCount = 0;
        Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(scanMatch));
        for (RedisFuture<String> future : keyFutures) {
            String key = future.get(commandTimeout, TimeUnit.SECONDS);
            if (key == null) {
                continue;
            }
            if (pattern.matcher(key).matches()) {
                matchCount++;
            }
        }
        float rate = (float) matchCount / sampleSize;
        long dbsize = dbsizeFuture.get(commandTimeout, TimeUnit.SECONDS);
        int matchSize = Math.round(dbsize * rate);
        setSize(matchSize);
        ScanArgs scanArgs = ScanArgs.Builder.limit(scanCount).match(scanMatch);
        this.iterator = ScanIterator.scan((RedisKeyCommands<K, V>) sync(connection), scanArgs);
    }

    protected abstract BaseRedisAsyncCommands<K, V> async(C connection) throws Exception;

    protected abstract BaseRedisCommands<K, V> sync(C connection) throws Exception;

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
