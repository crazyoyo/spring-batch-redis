package com.redis.spring.batch.reader;

import java.util.function.Consumer;

public interface KeyspaceNotificationPublisher extends AutoCloseable {

    void open();

    @Override
    void close();

    void addConsumer(Consumer<KeyspaceNotification> consumer);

}
