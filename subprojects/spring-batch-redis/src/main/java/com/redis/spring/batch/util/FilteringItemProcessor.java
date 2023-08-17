package com.redis.spring.batch.util;

import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;

/**
 * ItemProcessor that retains items matching the given predicate, i.e. a given item is kept only if predicate.test(item) == true. 
 * @param <T>
 */
public class FilteringItemProcessor<T> implements ItemProcessor<T, T> {

    private final Predicate<T> predicate;

    public FilteringItemProcessor(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public T process(T item) throws Exception {
        if (predicate.test(item)) {
            return item;
        }
        return null;
    }

}
