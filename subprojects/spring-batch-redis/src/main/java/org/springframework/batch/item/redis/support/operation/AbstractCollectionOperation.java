package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public abstract class AbstractCollectionOperation<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Predicate<T> remove;

    protected AbstractCollectionOperation(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove) {
        super(key, delete);
        this.remove = remove;
    }

    @Override
    protected RedisFuture<?> doExecute(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        if (remove.test(item)) {
            return remove(commands, item, key);
        }
        return add(commands, item, key);
    }

    protected abstract RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key);

    protected abstract RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key);

}
