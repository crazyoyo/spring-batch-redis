package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public abstract class AbstractCollectionOperation<K, V, S> extends AbstractKeyOperation<K, V, S, V> {

    private final Predicate<S> remove;

    protected AbstractCollectionOperation(Converter<S, K> key, Converter<S, V> member, Predicate<S> delete, Predicate<S> remove) {
        super(key, member, delete);
        this.remove = remove;
    }

    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, S item, K key, V member) {
        if (remove.test(item)) {
            return remove(commands, key, member);
        }
        return add(commands, item, key, member);
    }

    protected abstract RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, K key, V member);

    protected abstract RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, S item, K key, V member);


}
