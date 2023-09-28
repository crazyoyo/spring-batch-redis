package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

    private Function<T, Collection<ScoredValue<V>>> valuesFunction;

    private Function<T, ZAddArgs> argsFunction = t -> null;

    public void setArgs(ZAddArgs args) {
        this.argsFunction = t -> args;
    }

    public void setArgsFunction(Function<T, ZAddArgs> function) {
        this.argsFunction = function;
    }

    public void setValuesFunction(Function<T, Collection<ScoredValue<V>>> function) {
        this.valuesFunction = function;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Collection<ScoredValue<V>> values = valuesFunction.apply(item);
        if (CollectionUtils.isEmpty(values)) {
            return null;
        }
        ZAddArgs args = argsFunction.apply(item);
        ScoredValue<V>[] array = values.toArray(new ScoredValue[0]);
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, array);
    }

}
