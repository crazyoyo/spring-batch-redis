package org.springframework.batch.item.redis.support;

import org.springframework.core.convert.converter.Converter;

public class ConstantConverter<S, T> implements Converter<S, T> {

    private final T constant;

    public ConstantConverter(T constant) {
	this.constant = constant;
    }

    @Override
    public T convert(S source) {
	return constant;
    }

}
