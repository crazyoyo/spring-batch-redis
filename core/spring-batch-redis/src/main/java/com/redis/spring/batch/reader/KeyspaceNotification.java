package com.redis.spring.batch.reader;

public class KeyspaceNotification {

    private String key;

    private KeyEvent event;

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

}
