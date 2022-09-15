package com.redis.spring.batch.common;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class AndPredicate<T> implements Predicate<T> {

	private final List<? extends Predicate<? super T>> components;

	public AndPredicate(List<? extends Predicate<? super T>> components) {
		this.components = components;
	}

	@Override
	public boolean test(T t) {
		for (Predicate<? super T> component : components) {
			if (!component.test(t)) {
				return false;
			}
		}
		return true;
	}

	@SafeVarargs
	public static <T> AndPredicate<T> of(Predicate<T>... components) {
		return new AndPredicate<>(Arrays.asList(components));
	}

}