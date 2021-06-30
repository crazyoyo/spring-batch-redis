package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Lpush<K,V,T> extends AbstractCollectionOperation<K,V,T> {

    public Lpush(K key, Converter<T, V> member) {
        this(new ConstantConverter<>(key), member);
    }

    public Lpush(Converter<T, K> key, Converter<T, V> member) {
        this(key, member, new ConstantPredicate<>(false), new ConstantPredicate<>(false));
    }

    public Lpush(Converter<T, K> key, Converter<T, V> member, Predicate<T> delete, Predicate<T> remove) {
        super(key, member, delete, remove);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisListAsyncCommands<K, V>) commands).lpush(key, member);
    }

    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, K key, V member) {
        return ((RedisListAsyncCommands<K, V>) commands).lrem(key, 1, member);
    }
}
