package org.springframework.batch.item.redis.support;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public abstract class ScanKeyValueItemReaderBuilder<B extends ScanKeyValueItemReaderBuilder<B>> extends KeyValueItemReaderBuilder<B> {

    protected int sampleSize = DEFAULT_SAMPLE_SIZE;
    protected long scanCount = DEFAULT_SCAN_COUNT;

    public B scanCount(long scanCount) {
        this.scanCount = scanCount;
        return (B) this;
    }

    public B sampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
        return (B) this;
    }

    protected Predicate<String> keyPatternPredicate() {
        Pattern pattern = Pattern.compile(GlobToRegexConverter.convert(keyPattern));
        return k -> pattern.matcher(k).matches();
    }

}

