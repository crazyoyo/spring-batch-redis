package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Set<T> extends AbstractKeyOperation<T, String> {

    public Set(Converter<T, String> key, Converter<T, String> value) {
        this(key, value, new NullValuePredicate<>(value));
    }

    public Set(Converter<T, String> key, Converter<T, String> value, Predicate<T> delete) {
        super(key, value, delete);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, String value) {
        return ((RedisStringAsyncCommands<String, String>) commands).set(key, value);
    }

}
