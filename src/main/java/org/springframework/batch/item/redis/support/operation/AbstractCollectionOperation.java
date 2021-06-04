package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public abstract class AbstractCollectionOperation<T> extends AbstractKeyOperation<T, String> {

    private final Predicate<T> remove;

    protected AbstractCollectionOperation(Converter<T, String> key, Converter<T, String> member, Predicate<T> delete, Predicate<T> remove) {
        super(key, member, delete);
        this.remove = remove;
    }

    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, String member) {
        if (remove.test(item)) {
            return remove(commands, key, member);
        }
        return add(commands, item, key, member);
    }

    protected abstract RedisFuture<?> remove(BaseRedisAsyncCommands<String, String> commands, String key, String member);

    protected abstract RedisFuture<?> add(BaseRedisAsyncCommands<String, String> commands, T item, String key, String member);


}
