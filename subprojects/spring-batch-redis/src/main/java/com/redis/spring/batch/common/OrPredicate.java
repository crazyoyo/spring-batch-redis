package com.redis.spring.batch.common;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class OrPredicate<T> implements Predicate<T> {

	private final List<? extends Predicate<? super T>> components;

	public OrPredicate(List<? extends Predicate<? super T>> components) {
		this.components = components;
	}

	@Override
	public boolean test(T t) {
		for (Predicate<? super T> component : components) {
			if (component.test(t)) {
				return true;
			}
		}
		return false;
	}

	@SafeVarargs
	public static <T> OrPredicate<T> of(Predicate<T>... components) {
		return new OrPredicate<>(Arrays.asList(components));
	}

}