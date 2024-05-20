package com.redis.spring.batch.item.redis.reader;

public interface KeyNotificationConsumer<K, V> {

	void accept(K channel, V message);

}