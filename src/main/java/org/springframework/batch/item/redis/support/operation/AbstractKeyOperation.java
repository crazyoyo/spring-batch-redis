package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public abstract class AbstractKeyOperation<T, V> implements OperationItemWriter.RedisOperation<T> {

    private final Converter<T, String> key;
    private final Converter<T, V> value;
    private final Predicate<T> delete;

    protected AbstractKeyOperation(Converter<T, String> key, Converter<T, V> value, Predicate<T> delete) {
        Assert.notNull(key, "A key converter is required");
        Assert.notNull(value, "A value converter is required");
        Assert.notNull(delete, "A delete predicate is required");
        this.key = key;
        this.value = value;
        this.delete = delete;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        String key = this.key.convert(item);
        if (delete.test(item)) {
            return ((RedisKeyAsyncCommands<String, String>) commands).del(key);
        }
        V value = this.value.convert(item);
        return execute(commands, item, key, value);
    }

    protected abstract RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, V value);

}
