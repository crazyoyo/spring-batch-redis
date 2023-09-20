package com.redis.spring.batch.reader;

public interface ChannelMessagePublisher extends AutoCloseable {

    void open();

    @Override
    void close();

    void subscribe(ChannelMessageConsumer consumer, String pattern);

}
