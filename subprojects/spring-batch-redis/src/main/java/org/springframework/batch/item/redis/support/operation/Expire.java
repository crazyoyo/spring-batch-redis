package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Expire<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, Long> milliseconds;

    public Expire(Converter<T, K> key, Predicate<T> delete, Converter<T, Long> millis) {
        super(key, delete);
        Assert.notNull(millis, "A milliseconds converter is required");
        this.milliseconds = millis;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
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
