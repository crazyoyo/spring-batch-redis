package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Expire<T> extends AbstractKeyOperation<T, Long> {

    public Expire(Converter<T, String> key, Converter<T, Long> milliseconds) {
        this(key, milliseconds, new NonexistentKeyPredicate<>(milliseconds));
    }

    public Expire(Converter<T, String> key, Converter<T, Long> milliseconds, Predicate<T> delete) {
        super(key, milliseconds, delete);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, Long milliseconds) {
        if (milliseconds == null) {
            return null;
        }
        if (milliseconds < 0) {
            return null;
        }
        return ((RedisKeyAsyncCommands<String, String>) commands).pexpire(key, milliseconds);
    }


}
