package com.redis.spring.batch.gen;

import com.redis.spring.batch.common.Range;

public class StreamOptions {

    public static final Range DEFAULT_MESSAGE_COUNT = Range.of(10);

    private Range messageCount = DEFAULT_MESSAGE_COUNT;

    private MapOptions bodyOptions = new MapOptions();

    public Range getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(Range count) {
        this.messageCount = count;
    }

    public MapOptions getBodyOptions() {
        return bodyOptions;
    }

    public void setBodyOptions(MapOptions options) {
        this.bodyOptions = options;
    }

}
