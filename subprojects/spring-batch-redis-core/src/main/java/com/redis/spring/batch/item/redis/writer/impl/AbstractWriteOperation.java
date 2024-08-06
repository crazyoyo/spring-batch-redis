package com.redis.spring.batch.item.redis.writer.impl;

import java.util.function.Function;

import com.redis.spring.batch.item.redis.common.Operation;

public abstract class AbstractWriteOperation<K, V, T> implements Operation<K, V, T, Object> {

	protected final Function<T, K> keyFunction;

	protected AbstractWriteOperation(Function<T, K> keyFunction) {
		this.keyFunction = keyFunction;
	}

	protected K key(T item) {
		return keyFunction.apply(item);
	}

}
