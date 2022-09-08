package com.redis.spring.batch.reader;

public interface KeyspaceNotificationPublisher<K> {

	@SuppressWarnings("unchecked")
	void subscribe(K... patterns);

	@SuppressWarnings("unchecked")
	void unsubscribe(K... patterns);

	void addListener(KeyspaceNotificationListener<K> listener);

}
