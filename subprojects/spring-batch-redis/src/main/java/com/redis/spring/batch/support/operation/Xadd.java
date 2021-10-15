package com.redis.spring.batch.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.function.Predicate;

public class Xadd<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, XAddArgs> args;
    private final Converter<T, Map<K, V>> body;

    public Xadd(Converter<T, K> key, Predicate<T> delete, Converter<T, Map<K, V>> body, Converter<T, XAddArgs> args) {
        super(key, delete);
        Assert.notNull(body, "A body converter is required");
        Assert.notNull(args, "A XAddArgs converter is required");
        this.body = body;
        this.args = args;
    }

    @Override
    protected RedisFuture<?> doExecute(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        return commands.xadd(key, args.convert(item), body.convert(item));
    }

    public static <T> XaddBodyBuilder<String, T> key(String key) {
        return key(t -> key);
    }

    public static <K, T> XaddBodyBuilder<K, T> key(K key) {
        return key(t -> key);
    }

    public static <K, T> XaddBodyBuilder<K, T> key(Converter<T, K> key) {
        return new XaddBodyBuilder<>(key);
    }

    public static class XaddBodyBuilder<K, T> {

        private final Converter<T, K> key;

        public XaddBodyBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> XaddBuilder<K, V, T> body(Converter<T, Map<K, V>> body) {
            return new XaddBuilder<>(key, body);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class XaddBuilder<K, V, T> extends DelBuilder<K, V, T, XaddBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, Map<K, V>> body;
        private XAddArgs args;

        public XaddBuilder(Converter<T, K> key, Converter<T, Map<K, V>> body) {
            super(body);
            this.key = key;
            this.body = body;
        }

        @Override
        public Xadd<K, V, T> build() {
            return new Xadd<>(key, del, body, t -> args);
        }
    }

}
