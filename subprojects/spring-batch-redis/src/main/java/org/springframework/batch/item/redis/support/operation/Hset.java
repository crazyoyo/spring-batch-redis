package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.function.Predicate;

public class Hset<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, Map<K, V>> map;

    public Hset(Converter<T, K> key, Predicate<T> delete, Converter<T, Map<K, V>> map) {
        super(key, delete);
        Assert.notNull(map, "A map converter is required");
        this.map = map;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisHashAsyncCommands<K, V>) commands).hset(key, map.convert(item));
    }

}

