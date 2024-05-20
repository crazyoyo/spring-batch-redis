package com.redis.spring.batch.item.redis.reader;

public interface KeyEventListener<K> {

	void event(K key, String event, KeyNotificationStatus status);

}