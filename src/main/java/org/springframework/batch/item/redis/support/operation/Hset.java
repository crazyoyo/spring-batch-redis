package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;
import java.util.function.Predicate;

public class Hset<K,V,T> extends AbstractKeyOperation<K,V,T, Map<K, V>> {

    public Hset(Converter<T, K> key, Converter<T, Map<K, V>> value) {
        super(key, value, new NullValuePredicate<>(value));
    }

    public Hset(Converter<T, K> key, Converter<T, Map<K, V>> value, Predicate<T> delete) {
        super(key, value, delete);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Map<K, V> value) {
        return ((RedisHashAsyncCommands<K, V>) commands).hset(key, value);
    }

}

