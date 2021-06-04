package org.springframework.batch.item.redis.support.operation;

import org.springframework.core.convert.converter.Converter;

public class ConstantConverter<S, T> implements Converter<S, T> {

    private final T value;

    public ConstantConverter(T value) {
        this.value = value;
    }

    @Override
    public T convert(S source) {
        return value;
    }
}
