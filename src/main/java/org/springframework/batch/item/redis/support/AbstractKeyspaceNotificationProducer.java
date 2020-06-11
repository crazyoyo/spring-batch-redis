package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractKeyspaceNotificationProducer<K> implements KeyspaceNotificationProducer<K> {

    private final Converter<K, K> keyExtractor;
    private final List<KeyListener<K>> listeners = new ArrayList<>();

    protected AbstractKeyspaceNotificationProducer(Converter<K, K> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void addListener(KeyListener<K> listener) {
        this.listeners.add(listener);
    }

    @Override
    public void removeListener(KeyListener<K> listener) {
        this.listeners.remove(listener);
    }

    protected void notification(K notification) {
        listeners.forEach(l -> l.key(keyExtractor.convert(notification)));
    }
}
