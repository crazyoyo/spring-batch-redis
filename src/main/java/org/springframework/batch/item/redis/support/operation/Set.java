package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Set<T> extends AbstractKeyOperation<T> {

    @NonNull
    private final Converter<T, String> value;

    @Builder
    public Set(Converter<T, String> key, Converter<T, String> value) {
        super(key);
        Assert.notNull(value, "A value converter is required");
        this.value = value;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisStringAsyncCommands<String, String>) commands).set(key.convert(item), value.convert(item));
    }

}