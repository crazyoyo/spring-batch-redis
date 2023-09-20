package com.redis.spring.batch.reader;

public interface ChannelMessageConsumer {

    void message(String channel, String message);

}
