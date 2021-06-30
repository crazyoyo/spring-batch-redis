package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public abstract class AbstractKeyOperation<K, V, S, T> implements OperationItemWriter.RedisOperation<K, V, S> {

    private final Converter<S, K> key;
    private final Converter<S, T> value;
    private final Predicate<S> delete;

    protected AbstractKeyOperation(Converter<S, K> key, Converter<S, T> value, Predicate<S> delete) {
        Assert.notNull(key, "A key converter is required");
        Assert.notNull(value, "A value converter is required");
        Assert.notNull(delete, "A delete predicate is required");
        this.key = key;
        this.value = value;
        this.delete = delete;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, S item) {
        K key = this.key.convert(item);
        if (delete.test(item)) {
            return ((RedisKeyAsyncCommands<K, V>) commands).del(key);
        }
        T value = this.value.convert(item);
        return execute(commands, item, key, value);
    }

    protected abstract RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, S item, K key, T value);

}
