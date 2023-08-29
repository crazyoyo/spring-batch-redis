package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> extends AbstractOperation<K, V, T, ZaddAll<K, V, T>> {

    private Function<T, Collection<ScoredValue<V>>> values;

    private ZAddArgs args = null;

    public ZaddAll<K, V, T> args(ZAddArgs args) {
        this.args = args;
        return this;
    }

    public ZaddAll<K, V, T> values(Function<T, Collection<ScoredValue<V>>> values) {
        this.values = values;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        Collection<ScoredValue<V>> collection = values(item);
        if (!collection.isEmpty()) {
            futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key(item), args,
                    collection.toArray(new ScoredValue[0])));
        }
    }

    private Collection<ScoredValue<V>> values(T item) {
        return values.apply(item);
    }

}
