package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.function.Predicate;
import java.util.regex.Pattern;

@Setter
@Accessors(fluent = true)
public abstract class ScanKeyValueItemReaderBuilder<T extends AbstractKeyValueItemReader<String, String, ?>> extends AbstractKeyValueItemReader.AbstractKeyValueItemReaderBuilder<T, ScanKeyValueItemReaderBuilder<T>> {

    public static final String DEFAULT_SCAN_MATCH = "*";
    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final int DEFAULT_SAMPLE_SIZE = 100;

    private String scanMatch = DEFAULT_SCAN_MATCH;
    private long scanCount = DEFAULT_SCAN_COUNT;
    private int sampleSize = DEFAULT_SAMPLE_SIZE;

    private Predicate<String> keyPatternPredicate() {
        Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(scanMatch));
        return k -> pattern.matcher(k).matches();
    }

    public ScanKeyItemReader<String, String> keyReader(StatefulRedisConnection<String, String> connection) {
        return new ScanKeyItemReader<>(connection, c -> ((StatefulRedisConnection<String, String>) c).async(), c -> ((StatefulRedisConnection<String, String>) c).sync(), commandTimeout, scanCount, scanMatch, sampleSize, keyPatternPredicate());
    }

    public ScanKeyItemReader<String, String> keyReader(StatefulRedisClusterConnection<String, String> connection) {
        return new ScanKeyItemReader<>(connection, c -> ((StatefulRedisClusterConnection<String, String>) c).async(), c -> ((StatefulRedisClusterConnection<String, String>) c).sync(), commandTimeout, scanCount, scanMatch, sampleSize, keyPatternPredicate());
    }

}