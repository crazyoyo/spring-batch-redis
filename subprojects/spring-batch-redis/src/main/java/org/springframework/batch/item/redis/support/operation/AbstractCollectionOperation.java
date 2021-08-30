package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public abstract class AbstractCollectionOperation<T> extends AbstractKeyOperation<T> {

    private final Converter<T, Object> member;
    private final Predicate<T> remove;

    protected AbstractCollectionOperation(Converter<T, Object> key, Predicate<T> delete, Converter<T, Object> member, Predicate<T> remove) {
        super(key, delete);
        this.member = member;
        this.remove = remove;
    }

    @Override
    protected <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        V member = (V) this.member.convert(item);
        if (remove.test(item)) {
            return remove(commands, item, key, member);
        }
        return add(commands, item, key, member);
    }

    protected abstract <K, V> RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member);

    protected abstract <K, V> RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key, V member);

}
