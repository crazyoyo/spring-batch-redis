package com.redis.spring.batch.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Expire<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, Long> milliseconds;

    public Expire(Converter<T, K> key, Predicate<T> delete, Converter<T, Long> millis) {
        super(key, delete);
        Assert.notNull(millis, "A milliseconds converter is required");
        this.milliseconds = millis;
    }

    @Override
    protected RedisFuture<?> doExecute(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        Long millis = milliseconds.convert(item);
        if (millis == null) {
            return null;
        }
        if (millis < 0) {
            return null;
        }
        return commands.pexpire(key, millis);
    }

    public static <K, T> ExpireMillisBuilder<K, T> key(Converter<T, K> key) {
        return new ExpireMillisBuilder<>(key);
    }

    public static class ExpireMillisBuilder<K, T> {

        private final Converter<T, K> key;

        public ExpireMillisBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> ExpireBuilder<K, V, T> millis(Converter<T, Long> millis) {
            return new ExpireBuilder<>(key, millis);
        }
    }


    public static class ExpireBuilder<K, V, T> extends DelBuilder<K, V, T, ExpireBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, Long> millis;

        public ExpireBuilder(Converter<T, K> key, Converter<T, Long> millis) {
            super(millis);
            this.key = key;
            this.millis = millis;
        }

        @Override
        public Expire<K, V, T> build() {
            return new Expire<>(key, del, millis);
        }

    }

}
