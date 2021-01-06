package org.springframework.batch.item.redis.support;

public abstract class ScanKeyValueItemReaderBuilder<B extends ScanKeyValueItemReaderBuilder<B>> extends KeyValueItemReaderBuilder<B> {

    protected long scanCount = DEFAULT_SCAN_COUNT;

    public B scanCount(long scanCount) {
        this.scanCount = scanCount;
        return (B) this;
    }

}

