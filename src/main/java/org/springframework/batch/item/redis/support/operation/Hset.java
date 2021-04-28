package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;

@Setter
@Accessors(fluent = true)
public class Hset<T> extends AbstractKeyOperation<T> {

    @NonNull
    private final Converter<T, Map<String, String>> map;

    @Builder
    public Hset(Converter<T, String> key, Converter<T, Map<String, String>> map) {
        super(key);
        Assert.notNull(map, "A map converter is required");
        this.map = map;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisHashAsyncCommands<String, String>) commands).hset(key.convert(item), map.convert(item));
    }
}

