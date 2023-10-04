package com.redis.spring.batch.reader;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeyspaceNotification {

    private static final String SEPARATOR = ":";

    private static final Map<String, KeyEvent> EVENTS = Stream.of(KeyEvent.values())
            .collect(Collectors.toMap(KeyEvent::getString, Function.identity()));

    private String key;

    private KeyEvent event;

    public KeyspaceNotification() {
    }

    private KeyspaceNotification(Builder builder) {
        this.key = builder.key;
        this.event = builder.event;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public KeyEvent getEvent() {
        return event;
    }

    public void setEvent(KeyEvent event) {
        this.event = event;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KeyspaceNotification)) {
            return false;
        }
        KeyspaceNotification that = (KeyspaceNotification) obj;
        return key.equals(that.key);
    }

    @Override
    public String toString() {
        return "KeyspaceNotification [key=" + key + ", event=" + event + "]";
    }

    public static KeyspaceNotification of(String channel, String message) {
        String key = channel.substring(channel.indexOf(SEPARATOR) + 1);
        KeyEvent event = EVENTS.getOrDefault(message, KeyEvent.UNKNOWN);
        return KeyspaceNotification.builder().key(key).event(event).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String key;

        private KeyEvent event;

        private Builder() {
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder event(KeyEvent event) {
            this.event = event;
            return this;
        }

        public KeyspaceNotification build() {
            return new KeyspaceNotification(this);
        }

    }

}
