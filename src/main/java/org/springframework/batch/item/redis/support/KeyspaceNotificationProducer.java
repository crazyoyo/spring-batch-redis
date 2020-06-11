package org.springframework.batch.item.redis.support;

public interface KeyspaceNotificationProducer<K> {

    void open();

    void close();

    void addListener(KeyListener<K> listener);

    void removeListener(KeyListener<K> listener);
}
