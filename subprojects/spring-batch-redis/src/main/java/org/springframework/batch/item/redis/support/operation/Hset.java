package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.function.Predicate;

public class Hset<T> extends AbstractKeyOperation<T> {

    private final Converter<T, Map> map;

    public Hset(Converter<T, Object> key, Converter<T, Map> map) {
        this(key, new NullValuePredicate<>(map), map);
    }

    public Hset(Converter<T, Object> key, Predicate<T> delete, Converter<T, Map> map) {
        super(key, delete);
        Assert.notNull(map, "A map converter is required");
        this.map = map;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisHashAsyncCommands<K, V>) commands).hset(key, (Map<K, V>) map.convert(item));
    }

}

