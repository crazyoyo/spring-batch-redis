package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.LongRange;

public class StreamOptions {

    public static final LongRange DEFAULT_MESSAGE_COUNT = LongRange.is(10);

    private LongRange messageCount = DEFAULT_MESSAGE_COUNT;

    private MapOptions bodyOptions = new MapOptions();

    public LongRange getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(LongRange count) {
        this.messageCount = count;
    }

    public MapOptions getBodyOptions() {
        return bodyOptions;
    }

    public void setBodyOptions(MapOptions options) {
        this.bodyOptions = options;
    }

}
