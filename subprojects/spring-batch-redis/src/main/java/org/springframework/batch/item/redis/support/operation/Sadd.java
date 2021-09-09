package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Sadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    public Sadd(Converter<T, K> key, Predicate<T> delete, Converter<T, V> member, Predicate<T> remove) {
        super(key, delete, member, remove);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisSetAsyncCommands<K, V>) commands).srem(key, member);
    }

}

