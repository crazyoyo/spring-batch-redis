package com.redis.spring.batch.item.redis.writer.impl;

import java.util.function.Function;

public abstract class AbstractValueWriteOperation<K, V, R, T> extends AbstractWriteOperation<K, V, T> {

	protected final Function<T, R> valueFunction;

	protected AbstractValueWriteOperation(Function<T, K> keyFunction, Function<T, R> valueFunction) {
		super(keyFunction);
		this.valueFunction = valueFunction;
	}

	protected R value(T item) {
		return valueFunction.apply(item);
	}
}
