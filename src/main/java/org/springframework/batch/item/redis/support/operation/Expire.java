package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

@Setter
@Accessors(fluent = true)
public class Expire<T> extends AbstractKeyOperation<T> {

    @NonNull
    private final Converter<T, Long> timeout;

    @Builder
    public Expire(Converter<T, String> key, Converter<T, Long> timeout) {
        super(key);
        Assert.notNull(timeout, "A timeout converter is required");
        this.timeout = timeout;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        Long timeout = this.timeout.convert(item);
        if (timeout == null) {
            return null;
        }
        return ((RedisKeyAsyncCommands<String, String>) commands).pexpire(key.convert(item), timeout);
    }

}