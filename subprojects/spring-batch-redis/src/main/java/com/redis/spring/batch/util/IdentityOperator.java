package com.redis.spring.batch.util;

import java.util.function.UnaryOperator;

public class IdentityOperator<T> implements UnaryOperator<T> {

	@Override
	public T apply(T t) {
		return t;
	}

}
