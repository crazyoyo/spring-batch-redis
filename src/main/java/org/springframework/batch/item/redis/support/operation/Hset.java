package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;
import java.util.function.Predicate;

public class Hset<T> extends AbstractKeyOperation<T, Map<String, String>> {

    public Hset(Converter<T, String> key, Converter<T, Map<String, String>> value) {
        super(key, value, new NullValuePredicate<>(value));
    }

    public Hset(Converter<T, String> key, Converter<T, Map<String, String>> value, Predicate<T> delete) {
        super(key, value, delete);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, Map<String, String> value) {
        return ((RedisHashAsyncCommands<String, String>) commands).hset(key, value);
    }

}

