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
public class Xadd<T> extends AbstractKeyOperation<T, Map<String, String>> {

    private final Converter<T, XAddArgs> args;

    public Xadd(String key, Converter<T, Map<String, String>> body) {
        this(key, body, null);
    }

    public Xadd(String key, Converter<T, Map<String, String>> body, XAddArgs args) {
        this(new ConstantConverter<>(key), body, args);
    }

    public Xadd(Converter<T, String> key, Converter<T, Map<String, String>> body) {
        this(key, body, null);
    }

    public Xadd(Converter<T, String> key, Converter<T, Map<String, String>> body, XAddArgs args) {
        this(key, body, new ConstantPredicate<>(false), new ConstantConverter<>(args));
    }

    public Xadd(Converter<T, String> key, Converter<T, Map<String, String>> body, Predicate<T> delete, Converter<T, XAddArgs> args) {
        super(key, body, delete);
        Assert.notNull(args, "A XAddArgs converter is required");
        this.args = args;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, Map<String, String> value) {
        return ((RedisStreamAsyncCommands<String, String>) commands).xadd(key, args.convert(item), value);
    }

}
