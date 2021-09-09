package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class JsonSet<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, K> path;
    private final Converter<T, V> value;

    public JsonSet(Converter<T, K> key, Predicate<T> delete, Converter<T, K> path, Converter<T, V> value) {
        super(key, delete);
        this.path = path;
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisJSONAsyncCommands<K, V>) commands).set(key, path.convert(item), value.convert(item));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> delete(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key, path.convert(item));
    }
}
