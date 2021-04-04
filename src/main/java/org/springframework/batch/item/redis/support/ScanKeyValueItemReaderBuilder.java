package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public abstract class ScanKeyValueItemReaderBuilder<B extends ScanKeyValueItemReaderBuilder<B>> extends AbstractKeyValueItemReader.KeyValueItemReaderBuilder<B> {

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

    protected ScanKeyItemReader<String, String> keyReader(StatefulRedisConnection<String, String> connection) {
        return new ScanKeyItemReader<>(connection, c -> ((StatefulRedisConnection<String, String>) c).sync(), scanCount, scanMatch, scanType);
    }

    protected ScanKeyItemReader<String, String> keyReader(StatefulRedisClusterConnection<String, String> connection) {
        return new ScanKeyItemReader<>(connection, c -> ((StatefulRedisClusterConnection<String, String>) c).sync(), scanCount, scanMatch, scanType);
    }

}