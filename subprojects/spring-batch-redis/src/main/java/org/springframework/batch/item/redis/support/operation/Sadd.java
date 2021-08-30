package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Sadd<T> extends AbstractCollectionOperation<T> {

    public Sadd(Converter<T, Object> key, Converter<T, Object> member) {
        this(key, t -> false, member, t -> false);
    }

    public Sadd(Converter<T, Object> key, Predicate<T> delete, Converter<T, Object> member, Predicate<T> remove) {
        super(key, delete, member, remove);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisSetAsyncCommands<K, V>) commands).srem(key, member);
    }

}

