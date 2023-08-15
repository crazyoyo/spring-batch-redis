package com.redis.spring.batch;

import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;

public class PredicateItemProcessor<T> implements ItemProcessor<T, T> {

    private final Predicate<T> predicate;

    public PredicateItemProcessor(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public T process(T item) {
        return predicate.test(item) ? item : null;
    }

    public static <T> PredicateItemProcessor<T> of(Predicate<T> predicate) {
        return new PredicateItemProcessor<>(predicate);
    }

}
