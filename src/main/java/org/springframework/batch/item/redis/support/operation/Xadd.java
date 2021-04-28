package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;

@SuppressWarnings("unchecked")
public class Xadd<T> extends AbstractKeyOperation<T> {

    private final Converter<T, Map<String, String>> body;
    private final Converter<T, XAddArgs> args;

    public Xadd(Converter<T, String> key, Converter<T, Map<String, String>> body, Converter<T, XAddArgs> args) {
        super(key);
        Assert.notNull(body, "A body converter is required");
        Assert.notNull(args, "A XAddArgs converter is required");
        this.body = body;
        this.args = args;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        return ((RedisStreamAsyncCommands<String, String>) commands).xadd(key.convert(item), args.convert(item), body.convert(item));
    }

    public static <T> XaddBuilder<T> builder() {
        return new XaddBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class XaddBuilder<T> extends KeyOperationBuilder<T, XaddBuilder<T>> {

        private Converter<T, Map<String, String>> body;
        private Converter<T, XAddArgs> args = t -> null;

        public Xadd<T> build() {
            return new Xadd<>(key, body, args);
        }


    }

}