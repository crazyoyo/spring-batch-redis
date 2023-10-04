package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class AbstractKeyspaceNotificationPublisher implements KeyspaceNotificationPublisher {

    private final List<Consumer<KeyspaceNotification>> consumers = new ArrayList<>();

    @Override
    public void addConsumer(Consumer<KeyspaceNotification> consumer) {
        consumers.add(consumer);
    }

    protected void notification(String channel, String message) {
        KeyspaceNotification notification = KeyspaceNotification.of(channel, message);
        consumers.forEach(c -> c.accept(notification));
    }

}
