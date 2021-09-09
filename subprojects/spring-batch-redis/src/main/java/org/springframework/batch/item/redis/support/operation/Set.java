package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Set<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, V> value;

    public Set(Converter<T, K> key, Predicate<T> delete, Converter<T, V> value) {
        super(key, delete);
        Assert.notNull(value, "A value converter is required");
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisStringAsyncCommands<K, V>) commands).set(key, (V) value.convert(item));
    }

}
