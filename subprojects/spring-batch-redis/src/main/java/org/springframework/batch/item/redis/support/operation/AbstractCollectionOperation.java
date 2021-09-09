package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public abstract class AbstractCollectionOperation<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, V> member;
    private final Predicate<T> remove;

    protected AbstractCollectionOperation(Converter<T, K> key, Predicate<T> delete, Converter<T, V> member, Predicate<T> remove) {
        super(key, delete);
        this.member = member;
        this.remove = remove;
    }

    @Override
    protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        V member = this.member.convert(item);
        if (remove.test(item)) {
            return remove(commands, item, key, member);
        }
        return add(commands, item, key, member);
    }

    protected abstract RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member);

    protected abstract RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member);

}
