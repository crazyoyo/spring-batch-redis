package com.redis.spring.batch.gen;

import com.redis.spring.batch.util.IntRange;

public class StreamOptions {

    public static final IntRange DEFAULT_MESSAGE_COUNT = IntRange.is(10);

    private IntRange messageCount = DEFAULT_MESSAGE_COUNT;

    private MapOptions bodyOptions = new MapOptions();

    public IntRange getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(IntRange count) {
        this.messageCount = count;
    }

    public MapOptions getBodyOptions() {
        return bodyOptions;
    }

    public void setBodyOptions(MapOptions options) {
        this.bodyOptions = options;
    }

}
