package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ToListFunction<T, R> implements Function<List<T>, List<R>> {

    private final Function<T, R> function;

    public ToListFunction(Function<T, R> function) {
        this.function = function;
    }

    @Override
    public List<R> apply(List<T> t) {
        List<R> list = new ArrayList<>();
        for (T element : t) {
            list.add(function.apply(element));
        }
        return list;
    }

}
