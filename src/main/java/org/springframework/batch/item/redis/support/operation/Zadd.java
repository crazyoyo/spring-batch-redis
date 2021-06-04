package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Zadd<T> extends AbstractCollectionOperation<T> {

    private final Converter<T, Double> score;

    public Zadd(String key, Converter<T, String> member, Converter<T, Double> score) {
        this(new ConstantConverter<>(key), member, score);
    }

    public Zadd(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> score) {
        this(key, member, new ConstantPredicate<>(false), new NullValuePredicate<>(score), score);
    }

    public Zadd(Converter<T, String> key, Converter<T, String> member, Predicate<T> delete, Predicate<T> remove, Converter<T, Double> score) {
        super(key, member, delete, remove);
        Assert.notNull(score, "A score converter is required");
        this.score = score;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(BaseRedisAsyncCommands<String, String> commands, T item, String key, String member) {
        Double scoreValue = score.convert(item);
        if (scoreValue == null) {
            return null;
        }
        return ((RedisSortedSetAsyncCommands<String, String>) commands).zadd(key, scoreValue, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<String, String> commands, String key, String member) {
        return ((RedisSortedSetAsyncCommands<String, String>) commands).zrem(key, member);
    }
}
