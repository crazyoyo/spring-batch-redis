package org.springframework.batch.item.redis.support.operation;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class NonexistentKeyPredicate<T> implements Predicate<T> {

    public static final long TTL_KEY_DOES_NOT_EXIST = -2;

    private final Converter<T, Long> timeout;

    public NonexistentKeyPredicate(Converter<T, Long> timeout) {
        Assert.notNull(timeout, "A timeout converter is required");
        this.timeout = timeout;
    }

    @Override
    public boolean test(T item) {
        Long timeout = this.timeout.convert(item);
        if (timeout == null) {
            return false;
        }
        return timeout == TTL_KEY_DOES_NOT_EXIST;
    }
}
