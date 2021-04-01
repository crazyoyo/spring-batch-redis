package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import java.util.function.Predicate;
import java.util.regex.Pattern;

@SuppressWarnings("rawtypes")
public abstract class ScanKeyValueItemReaderBuilder<B extends ScanKeyValueItemReaderBuilder<B>> extends AbstractKeyValueItemReader.KeyValueItemReaderBuilder<B> {

    public static final String DEFAULT_SCAN_MATCH = "*";
    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final int DEFAULT_SAMPLE_SIZE = 100;

    private String scanMatch = DEFAULT_SCAN_MATCH;
    private long scanCount = DEFAULT_SCAN_COUNT;
    private int sampleSize = DEFAULT_SAMPLE_SIZE;

    public B scanMatch(String scanMatch) {
        this.scanMatch = scanMatch;
        return (B) this;
    }

    public B scanCount(long scanCount) {
        this.scanCount = scanCount;
        return (B) this;
    }

    public B sampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
        return (B) this;
    }

    private Predicate<String> keyPatternPredicate() {
        Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(scanMatch));
        return k -> pattern.matcher(k).matches();
    }

    protected ScanKeyItemReader<String, String> keyReader(StatefulRedisConnection<String, String> connection) {
        return new ScanKeyItemReader<>(connection, c -> ((StatefulRedisConnection<String, String>) c).async(), c -> ((StatefulRedisConnection<String, String>) c).sync(), scanCount, scanMatch, sampleSize, keyPatternPredicate());
    }

    protected ScanKeyItemReader<String, String> keyReader(StatefulRedisClusterConnection<String, String> connection) {
        return new ScanKeyItemReader<>(connection, c -> ((StatefulRedisClusterConnection<String, String>) c).async(), c -> ((StatefulRedisClusterConnection<String, String>) c).sync(), scanCount, scanMatch, sampleSize, keyPatternPredicate());
    }

}