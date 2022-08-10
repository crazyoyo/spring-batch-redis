package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

abstract class DelBuilder<K, V, T, B extends DelBuilder<K, V, T, B>> {

	protected Predicate<T> del = t -> false;

	@SuppressWarnings("unchecked")
	public B onNull(Converter<T, ?> value) {
		this.del = t -> value.convert(t) == null;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B del(Predicate<T> del) {
		this.del = del;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B del(boolean delete) {
		this.del = t -> delete;
		return (B) this;
	}

}