package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import lombok.Builder;
import org.springframework.core.convert.converter.Converter;

public class Sadd<T> extends AbstractCollectionOperation<T> {

    @Builder
    public Sadd(Converter<T, String> key, Converter<T, String> member) {
        super(key, member);
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisSetAsyncCommands<String, String>) commands).sadd(key.convert(item), member.convert(item));
    }

}

