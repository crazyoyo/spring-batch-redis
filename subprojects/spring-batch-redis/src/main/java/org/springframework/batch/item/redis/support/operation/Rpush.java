package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Rpush<T> extends AbstractCollectionOperation<T> {

    public Rpush(Converter<T, Object> key, Predicate<T> delete, Converter<T, Object> member, Predicate<T> remove) {
        super(key, delete, member, remove);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisListAsyncCommands<K, V>) commands).rpush(key, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisListAsyncCommands<K, V>) commands).lrem(key, -1, member);
    }
}
