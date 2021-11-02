package com.redis.spring.batch.support.convert;

import java.lang.reflect.Array;

import org.springframework.core.convert.converter.Converter;

public class ArrayConverter<S, E> implements Converter<S, E[]> {

	private final Converter<S, ? extends E>[] elementArrayConverter;
	private Class<E> type;

	@SuppressWarnings("unchecked")
	public ArrayConverter(Class<E> type, Converter<S, ? extends E>... elements) {
		this.type = type;
		this.elementArrayConverter = elements;
	}

	@Override
	@SuppressWarnings("unchecked")
	public E[] convert(S source) {
		E[] array = (E[]) Array.newInstance(type, this.elementArrayConverter.length);
		for (int index = 0; index < array.length; index++) {
			array[index] = this.elementArrayConverter[index].convert(source);
		}
		return array;
	}

}
