package org.springframework.batch.item.redis.support.operation;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class NullValuePredicate<T> implements Predicate<T> {

    private final Converter<T, ?> value;

    public NullValuePredicate(Converter<T, ?> value) {
        Assert.notNull(value, "A value converter is required");
        this.value = value;
    }

    @Override
    public boolean test(T t) {
        return value.convert(t) == null;
    }

}
