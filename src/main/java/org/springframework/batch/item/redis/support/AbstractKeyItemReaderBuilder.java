package org.springframework.batch.item.redis.support;

public class AbstractKeyItemReaderBuilder<K, V, B extends AbstractKeyItemReaderBuilder<K, V, B>> extends CommandTimeoutBuilder<B> {

    public static final long DEFAULT_SCAN_COUNT = 1000;
    public static final String DEFAULT_SCAN_MATCH = "*";
    public static final int DEFAULT_SAMPLE_SIZE = 30;

    protected long scanCount = DEFAULT_SCAN_COUNT;
    protected String scanMatch = DEFAULT_SCAN_MATCH;
    protected int sampleSize = DEFAULT_SAMPLE_SIZE;

    public B scanCount(long scanCount) {
        this.scanCount = scanCount;
        return (B) this;
    }

    public B scanMatch(String scanMatch) {
        this.scanMatch = scanMatch;
        return (B) this;
    }

    public B sampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
        return (B) this;
    }

}
