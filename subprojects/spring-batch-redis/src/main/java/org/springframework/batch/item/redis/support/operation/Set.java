package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Set<K, V, T> extends AbstractKeyOperation<K, V, T, V> {

    public Set(Converter<T, K> key, Converter<T, V> value) {
        this(key, value, new NullValuePredicate<>(value));
    }

    public Set(Converter<T, K> key, Converter<T, V> value, Predicate<T> delete) {
        super(key, value, delete);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, V value) {
        return ((RedisStringAsyncCommands<K, V>) commands).set(key, value);
    }

}
