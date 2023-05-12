package com.redis.spring.batch.convert;

import java.util.function.Function;

public class ArrayConverter<S, E> implements Function<S, E[]> {

	private final Function<S, ? extends E>[] elements;

	@SuppressWarnings("unchecked")
	public ArrayConverter(Function<S, ? extends E>... elements) {
		this.elements = elements;
	}

	@Override
	@SuppressWarnings("unchecked")
	public E[] apply(S t) {
		Object[] target = new Object[elements.length];
		for (int index = 0; index < elements.length; index++) {
			Function<S, ? extends E> converter = elements[index];
			target[index] = converter.apply(t);
		}
		return (E[]) target;
	}

}
