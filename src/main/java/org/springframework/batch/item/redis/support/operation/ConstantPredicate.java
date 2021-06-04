package org.springframework.batch.item.redis.support.operation;

import java.util.function.Predicate;

public class ConstantPredicate<T> implements Predicate<T> {

    private final boolean value;

    public ConstantPredicate(boolean value) {
        this.value = value;
    }

    @Override
    public boolean test(T t) {
        return value;
    }
}
