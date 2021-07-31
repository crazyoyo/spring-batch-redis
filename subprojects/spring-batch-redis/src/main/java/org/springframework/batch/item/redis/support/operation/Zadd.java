package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Zadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    private final Converter<T, Double> score;

    public Zadd(K key, Converter<T, V> member, Converter<T, Double> score) {
        this(new ConstantConverter<>(key), member, score);
    }

    public Zadd(Converter<T, K> key, Converter<T, V> member, Converter<T, Double> score) {
        this(key, member, new ConstantPredicate<>(false), new NullValuePredicate<>(score), score);
    }

    public Zadd(Converter<T, K> key, Converter<T, V> member, Predicate<T> delete, Predicate<T> remove, Converter<T, Double> score) {
        super(key, member, delete, remove);
        Assert.notNull(score, "A score converter is required");
        this.score = score;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member) {
        Double scoreValue = score.convert(item);
        if (scoreValue == null) {
            return null;
        }
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, scoreValue, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, K key, V member) {
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, member);
    }
}
