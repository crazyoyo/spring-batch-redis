package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public abstract class AbstractKeyOperation<S> implements OperationItemWriter.RedisOperation<S> {

    private final Converter<S, Object> key;
    private final Predicate<S> delete;

    protected AbstractKeyOperation(Converter<S, Object> key, Predicate<S> delete) {
        Assert.notNull(key, "A key converter is required");
        Assert.notNull(delete, "A delete predicate is required");
        this.key = key;
        this.delete = delete;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, S item) {
        K key = (K) this.key.convert(item);
        if (delete.test(item)) {
            return delete(commands, item, key);
        }
        return doExecute(commands, item, key);
    }

    @SuppressWarnings({"unchecked", "unused"})
    protected <K, V> RedisFuture<?> delete(BaseRedisAsyncCommands<K, V> commands, S item, K key) {
        return ((RedisKeyAsyncCommands<K, V>) commands).del(key);
    }

    protected abstract <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, S item, K key);


}
