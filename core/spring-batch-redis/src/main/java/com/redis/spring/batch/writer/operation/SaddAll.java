package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class SaddAll<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, Collection<V>> values;

    public void setValues(Function<T, Collection<V>> values) {
        this.values = values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        Collection<V> collection = values(item);
        if (!collection.isEmpty()) {
            futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(key(item), (V[]) collection.toArray()));
        }
    }

    private Collection<V> values(T item) {
        return values.apply(item);
    }

}
