package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractKeyspaceNotificationPublisher implements ChannelMessagePublisher {

    private final Map<String, List<ChannelMessageConsumer>> subscriptions = new HashMap<>();

    @Override
    public void subscribe(ChannelMessageConsumer consumer, String pattern) {
        subscriptions.computeIfAbsent(pattern, p -> new ArrayList<>()).add(consumer);
    }

    @Override
    public void open() {
        subscribe(patterns());
    }

    protected abstract void subscribe(String... patterns);

    private String[] patterns() {
        return subscriptions.keySet().toArray(new String[0]);
    }

    @Override
    public void close() {
        unsubscribe(patterns());
    }

    protected abstract void unsubscribe(String... patterns);

    protected void channelMessage(String pattern, String channel, String message) {
        if (subscriptions.containsKey(pattern)) {
            subscriptions.get(pattern).forEach(c -> c.message(channel, message));
        }
    }

}
