package org.springframework.batch.item.redis.support;

import io.lettuce.core.XReadArgs;

import java.time.Duration;

public class AbstractStreamItemReaderBuilder<K, V, B extends AbstractStreamItemReaderBuilder<K, V, B>> {

    public final static Duration DEFAULT_BLOCK = Duration.ofMillis(100);

    protected XReadArgs.StreamOffset<K> offset;
    protected Duration block = DEFAULT_BLOCK;
    protected Long count;
    protected boolean noack;

    public B offset(XReadArgs.StreamOffset<K> offset) {
        this.offset = offset;
        return (B) this;
    }

    public B block(Duration block) {
        this.block = block;
        return (B) this;
    }

    public B count(long count) {
        this.count = count;
        return (B) this;
    }

    public B noack(boolean noack) {
        this.noack = noack;
        return (B) this;
    }

}
