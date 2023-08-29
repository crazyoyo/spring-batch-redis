package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.spring.batch.writer.Operation;

public abstract class AbstractOperation<K, V, T, O extends AbstractOperation<K, V, T, O>> implements Operation<K, V, T> {

    private Function<T, K> key;

    @SuppressWarnings("unchecked")
    public O key(Function<T, K> function) {
        this.key = function;
        return (O) this;
    }

    protected K key(T item) {
        return key.apply(item);
    }

}
