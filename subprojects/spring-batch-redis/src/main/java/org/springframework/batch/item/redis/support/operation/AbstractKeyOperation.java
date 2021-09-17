package org.springframework.batch.item.redis.support.operation;

import java.util.function.Predicate;

import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.RedisFuture;

public abstract class AbstractKeyOperation<K, V, T> implements RedisOperation<K, V, T> {

    private final Converter<T, K> key;
    private final Predicate<T> delete;

    protected AbstractKeyOperation(Converter<T, K> key, Predicate<T> delete) {
        Assert.notNull(key, "A key converter is required");
        Assert.notNull(delete, "A delete predicate is required");
        this.key = key;
        this.delete = delete;
    }

    @Override
    public RedisFuture<?> execute(RedisModulesAsyncCommands<K, V> commands, T item) {
        K key = this.key.convert(item);
        if (delete.test(item)) {
            return delete(commands, item, key);
        }
        return doExecute(commands, item, key);
    }

    @SuppressWarnings("unchecked")
    protected RedisFuture<?> delete(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        return commands.del(key);
    }

    protected abstract RedisFuture<?> doExecute(RedisModulesAsyncCommands<K, V> commands, T item, K key);


}
