package com.redis.spring.batch.support;

public interface KeyMessageListener<K> {

	void message(K message);

}
