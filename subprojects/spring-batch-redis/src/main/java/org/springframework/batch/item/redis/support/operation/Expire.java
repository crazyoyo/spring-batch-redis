package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Expire<T> extends AbstractKeyOperation<T> {

    private final Converter<T, Long> milliseconds;

    public Expire(Converter<T, Object> key, Converter<T, Long> millis) {
        this(key, new NonexistentKeyPredicate<>(millis), millis);
    }

    public Expire(Converter<T, Object> key, Predicate<T> delete, Converter<T, Long> millis) {
        super(key, delete);
        Assert.notNull(millis, "A milliseconds converter is required");
        this.milliseconds = millis;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Long millis = milliseconds.convert(item);
        if (millis == null) {
            return null;
        }
        if (millis < 0) {
            return null;
        }
        return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, millis);
    }

}
