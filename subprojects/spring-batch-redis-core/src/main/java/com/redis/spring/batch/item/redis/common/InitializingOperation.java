package com.redis.spring.batch.item.redis.common;

import org.springframework.beans.factory.InitializingBean;

import io.lettuce.core.AbstractRedisClient;

public interface InitializingOperation<K, V, I, O> extends Operation<K, V, I, O>, InitializingBean {

	void setClient(AbstractRedisClient client);

}
