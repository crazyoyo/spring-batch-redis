package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class JsonSet<T> extends AbstractKeyOperation<T> {

    private final Converter<T, Object> path;
    private final Converter<T, Object> value;

    public JsonSet(Converter<T, Object> key, Converter<T, Object> path, Converter<T, Object> value) {
        this(key, new NullValuePredicate<>(value), path, value);
    }

    public JsonSet(Converter<T, Object> key, Predicate<T> delete, Converter<T, Object> path, Converter<T, Object> value) {
        super(key, delete);
        this.path = path;
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisJSONAsyncCommands<K, V>) commands).set(key, (K) path.convert(item), (V) value.convert(item));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> delete(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisJSONAsyncCommands<K, V>) commands).del(key, (K) path.convert(item));
    }
}
