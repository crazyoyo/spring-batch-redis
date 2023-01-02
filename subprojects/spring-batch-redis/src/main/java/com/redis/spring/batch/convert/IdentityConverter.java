package com.redis.spring.batch.convert;

import org.springframework.core.convert.converter.Converter;

public class IdentityConverter<T> implements Converter<T, T> {

	@SuppressWarnings("rawtypes")
	public static final IdentityConverter INSTANCE = new IdentityConverter<>();

	@Override
	public T convert(T source) {
		return source;
	}

	@SuppressWarnings("unchecked")
	public static <T> IdentityConverter<T> instance() {
		return INSTANCE;
	}

}
