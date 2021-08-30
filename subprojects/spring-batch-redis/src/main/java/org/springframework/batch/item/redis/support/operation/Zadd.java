package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Zadd<T> extends AbstractCollectionOperation<T> {

    private final Converter<T, Double> score;

    public Zadd(Converter<T, Object> key, Converter<T, Double> score, Converter<T, Object> member) {
        this(key, new ConstantPredicate<>(false), member, new NullValuePredicate<>(score), score);
    }

    public Zadd(Converter<T, Object> key, Predicate<T> delete, Converter<T, Object> member, Predicate<T> remove, Converter<T, Double> score) {
        super(key, delete, member, remove);
        Assert.notNull(score, "A score converter is required");
        this.score = score;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        Double scoreValue = score.convert(item);
        if (scoreValue == null) {
            return null;
        }
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, scoreValue, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, member);
    }
}
