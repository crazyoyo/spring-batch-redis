package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Expire<T> extends AbstractKeyOperation<T> {

    private final Converter<T, Long> timeout;

    public Expire(Converter<T, String> key, Converter<T, Long> timeout) {
        super(key);
        Assert.notNull(timeout, "A timeout converter is required");
        this.timeout = timeout;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        Long timeout = this.timeout.convert(item);
        if (timeout == null) {
            return null;
        }
        return ((RedisKeyAsyncCommands<String, String>) commands).pexpire(key.convert(item), timeout);
    }

    public static <T> ExpireBuilder<T> builder() {
        return new ExpireBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class ExpireBuilder<T> extends KeyOperationBuilder<T, ExpireBuilder<T>> {

        private Converter<T, Long> timeout;

        public Expire<T> build() {
            return new Expire<>(key, timeout);
        }

    }

}