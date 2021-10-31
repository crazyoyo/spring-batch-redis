package com.redis.spring.batch.support;

public interface PubSubSubscriber<K> {

	@SuppressWarnings("unchecked")
	void open(KeyMessageListener<K>... listeners);

	void close();

}
