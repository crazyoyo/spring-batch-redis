package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Sadd<T> extends AbstractCollectionOperation<T> {

    public Sadd(String key, Converter<T, String> member) {
        this(new ConstantConverter<>(key), member);
    }

    public Sadd(Converter<T, String> key, Converter<T, String> member) {
        this(key, member, new ConstantPredicate<>(false), new ConstantPredicate<>(false));
    }

    public Sadd(Converter<T, String> key, Converter<T, String> member, Predicate<T> delete, Predicate<T> remove) {
        super(key, member, delete, remove);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<?> add(BaseRedisAsyncCommands<String, String> commands, T item, String key, String member) {
        return ((RedisSetAsyncCommands<String, String>) commands).sadd(key, member);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<String, String> commands, String key, String member) {
        return ((RedisSetAsyncCommands<String, String>) commands).srem(key, member);
    }

}

