package com.redis.spring.batch.support.convert;

import java.lang.reflect.Array;

import org.springframework.core.convert.converter.Converter;

public class ArrayConverter<S, E> implements Converter<S, E[]> {

	private final Converter<S, ? extends E>[] elements;
	private Class<E> type;

	@SuppressWarnings("unchecked")
	public ArrayConverter(Class<E> type, Converter<S, ? extends E>... elements) {
		this.type = type;
		this.elements = elements;
	}

	@Override
	@SuppressWarnings("unchecked")
	public E[] convert(S source) {
		E[] elements = (E[]) Array.newInstance(type, this.elements.length);
		for (int index = 0; index < elements.length; index++) {
			elements[index] = this.elements[index].convert(source);
		}
		return elements;
	}

}
