package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, Collection<ScoredValue<V>>> values;

    private ZAddArgs args;

    public void setArgs(ZAddArgs args) {
        this.args = args;
    }

    public void setValues(Function<T, Collection<ScoredValue<V>>> values) {
        this.values = values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        Collection<ScoredValue<V>> collection = values(item);
        RedisSortedSetAsyncCommands<K, V> zsetCommands = (RedisSortedSetAsyncCommands<K, V>) commands;
        if (!collection.isEmpty()) {
            futures.add(zsetCommands.zadd(key(item), args, collection.toArray(new ScoredValue[0])));
        }
    }

    private Collection<ScoredValue<V>> values(T item) {
        return values.apply(item);
    }

}
