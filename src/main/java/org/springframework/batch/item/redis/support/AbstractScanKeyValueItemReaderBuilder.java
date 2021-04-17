package org.springframework.batch.item.redis.support;

import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.IteratorItemReader;

@SuppressWarnings({"unused", "unchecked"})
public abstract class AbstractScanKeyValueItemReaderBuilder<R extends AbstractKeyValueItemReader, B extends AbstractScanKeyValueItemReaderBuilder<R, B>> extends AbstractKeyValueItemReader.KeyValueItemReaderBuilder<R, B> {

    public static final String DEFAULT_SCAN_MATCH = "*";
    public static final long DEFAULT_SCAN_COUNT = 1000;

    private long scanCount = DEFAULT_SCAN_COUNT;
    private String scanMatch = DEFAULT_SCAN_MATCH;
    private String scanType;

    public B scanMatch(String scanMatch) {
        this.scanMatch = scanMatch;
        return (B) this;
    }

    public B scanType(String scanType) {
        this.scanType = scanType;
        return (B) this;
    }

    public B scanCount(long scanCount) {
        this.scanCount = scanCount;
        return (B) this;
    }

    protected ItemReader<String> keyReader(StatefulRedisConnection<String, String> connection) {
        return keyReader(connection.sync());
    }

    protected ItemReader<String> keyReader(StatefulRedisClusterConnection<String, String> connection) {
        return keyReader(connection.sync());
    }

    private <K, V> ItemReader<K> keyReader(BaseRedisCommands<K, V> commands) {
        KeyScanArgs args = KeyScanArgs.Builder.limit(scanCount);
        if (scanMatch != null) {
            args.match(scanMatch);
        }
        if (scanType != null) {
            args.type(scanType);
        }
        return new IteratorItemReader<>(ScanIterator.scan((RedisKeyCommands<K, V>) commands, args));
    }

}