package com.redis.spring.batch.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.springframework.core.convert.converter.Converter;

public class ArrayConverter<S, E> implements Converter<S, Collection<E>> {

	private final Collection<Converter<S, ? extends E>> converters;

	@SuppressWarnings("unchecked")
	public ArrayConverter(Converter<S, ? extends E>... elements) {
		this.converters = Arrays.asList(elements);
	}

	@Override
	public Collection<E> convert(S source) {
		Collection<E> target = new ArrayList<>();
		for (Converter<S, ? extends E> converter : converters) {
			target.add(converter.convert(source));
		}
		return target;
	}

}
