package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Expire<K, V, T> extends AbstractKeyOperation<K, V, T, Long> {

    public Expire(Converter<T, K> key, Converter<T, Long> milliseconds) {
        this(key, milliseconds, new NonexistentKeyPredicate<>(milliseconds));
    }

    public Expire(Converter<T, K> key, Converter<T, Long> milliseconds, Predicate<T> delete) {
        super(key, milliseconds, delete);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Long milliseconds) {
        if (milliseconds == null) {
            return null;
        }
        if (milliseconds < 0) {
            return null;
        }
        return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, milliseconds);
    }


}
