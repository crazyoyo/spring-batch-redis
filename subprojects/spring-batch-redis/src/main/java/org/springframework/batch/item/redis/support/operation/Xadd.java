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
public class Xadd<T> extends AbstractKeyOperation<T> {

    private final Converter<T, XAddArgs> args;
    private final Converter<T, Object> body;

    public Xadd(Converter<T, Object> key, Converter<T, Object> body) {
        this(key, body, null);
    }

    public Xadd(Converter<T, Object> key, Converter<T, Object> body, XAddArgs args) {
        this(key, t -> false, body, t -> args);
    }

    public Xadd(Converter<T, Object> key, Predicate<T> delete, Converter<T, Object> body, Converter<T, XAddArgs> args) {
        super(key, delete);
        Assert.notNull(args, "A XAddArgs converter is required");
        this.body = body;
        this.args = args;
    }

    @Override
    protected <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args.convert(item), (Map<K, V>) body.convert(item));
    }

}
