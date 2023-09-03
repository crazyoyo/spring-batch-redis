package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.spring.batch.writer.Operation;

public abstract class AbstractOperation<K, V, T> implements Operation<K, V, T> {

    private Function<T, K> key;

    public void setKey(Function<T, K> function) {
        this.key = function;
    }

    public void setKey(K key) {
        this.key = t -> key;
    }

    protected K key(T item) {
        return key.apply(item);
    }

}
