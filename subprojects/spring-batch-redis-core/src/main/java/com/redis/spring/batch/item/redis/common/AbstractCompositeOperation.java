package com.redis.spring.batch.item.redis.common;

import io.lettuce.core.AbstractRedisClient;

public abstract class AbstractCompositeOperation<K, V, I, O> implements InitializingOperation<K, V, I, O> {

	protected final Operation<K, V, I, O> delegate;

	protected AbstractCompositeOperation(Operation<K, V, I, O> delegate) {
		this.delegate = delegate;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (delegate instanceof InitializingOperation) {
			((InitializingOperation<K, V, I, O>) delegate).afterPropertiesSet();
		}
	}

	@Override
	public void setClient(AbstractRedisClient client) {
		if (delegate instanceof InitializingOperation) {
			((InitializingOperation<K, V, I, O>) delegate).setClient(client);
		}
	}

}
