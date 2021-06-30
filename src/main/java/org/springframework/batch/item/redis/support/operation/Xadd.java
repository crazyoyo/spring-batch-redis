package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.function.Predicate;

@SuppressWarnings("unchecked")
public class Xadd<K, V, T> extends AbstractKeyOperation<K, V, T, Map<K, V>> {

    private final Converter<T, XAddArgs> args;

    public Xadd(K key, Converter<T, Map<K, V>> body) {
        this(key, body, null);
    }

    public Xadd(K key, Converter<T, Map<K, V>> body, XAddArgs args) {
        this(new ConstantConverter<>(key), body, args);
    }

    public Xadd(Converter<T, K> key, Converter<T, Map<K, V>> body) {
        this(key, body, null);
    }

    public Xadd(Converter<T, K> key, Converter<T, Map<K, V>> body, XAddArgs args) {
        this(key, body, new ConstantPredicate<>(false), new ConstantConverter<>(args));
    }

    public Xadd(Converter<T, K> key, Converter<T, Map<K, V>> body, Predicate<T> delete, Converter<T, XAddArgs> args) {
        super(key, body, delete);
        Assert.notNull(args, "A XAddArgs converter is required");
        this.args = args;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Map<K, V> value) {
        return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args.convert(item), value);
    }

}
