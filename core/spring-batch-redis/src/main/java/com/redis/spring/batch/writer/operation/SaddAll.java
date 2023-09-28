package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class SaddAll<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, Collection<V>> valuesFunction;

    public void setValuesFunction(Function<T, Collection<V>> function) {
        this.valuesFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Collection<V> collection = valuesFunction.apply(item);
        if (CollectionUtils.isEmpty(collection)) {
            return null;
        }
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, (V[]) collection.toArray());
    }

}
