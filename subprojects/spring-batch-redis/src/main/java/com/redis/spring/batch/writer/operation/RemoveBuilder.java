package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

abstract class RemoveBuilder<K, V, T, B extends RemoveBuilder<K, V, T, B>> {

	protected Predicate<T> remove = t -> false;

	@SuppressWarnings("unchecked")
	public B remove(Predicate<T> remove) {
		this.remove = remove;
		return (B) this;
	}

}