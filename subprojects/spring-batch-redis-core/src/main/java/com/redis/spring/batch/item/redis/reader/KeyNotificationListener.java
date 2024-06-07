package com.redis.spring.batch.item.redis.reader;

public interface KeyNotificationListener<K> {

	void notification(KeyNotification<K> notification, KeyNotificationStatus status);

}
