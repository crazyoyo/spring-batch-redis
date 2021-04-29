package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Set<T> extends AbstractKeyOperation<T> {

    private final Converter<T, String> value;

    public Set(Converter<T, String> key, Converter<T, String> value) {
        super(key);
        Assert.notNull(value, "A value converter is required");
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisStringAsyncCommands<String, String>) commands).set(key.convert(item), value.convert(item));
    }

    public static <T> SetBuilder<T> builder() {
        return new SetBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class SetBuilder<T> extends KeyOperationBuilder<T, SetBuilder<T>> {

        private Converter<T, String> value;

        public Set<T> build() {
            return new Set<>(key, value);
        }

    }

}