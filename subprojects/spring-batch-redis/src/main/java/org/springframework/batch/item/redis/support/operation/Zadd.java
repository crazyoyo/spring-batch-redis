package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Zadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    private final Converter<T, ScoredValue<V>> value;
    private final ZAddArgs args;

    public Zadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, ScoredValue<V>> value, ZAddArgs args) {
        super(key, delete, remove);
        Assert.notNull(value, "A scored value converter is required");
        this.value = value;
        this.args = args;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        ScoredValue<V> scoredValue = value.convert(item);
        if (scoredValue == null) {
            return null;
        }
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, scoredValue);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        ScoredValue<V> scoredValue = value.convert(item);
        if (scoredValue == null) {
            return null;
        }
        return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, scoredValue.getValue());
    }

    public static <T> ZaddValueBuilder<String, T> key(String key) {
        return key(t -> key);
    }

    public static <K, T> ZaddValueBuilder<K, T> key(K key) {
        return key(t -> key);
    }

    public static <K, T> ZaddValueBuilder<K, T> key(Converter<T, K> key) {
        return new ZaddValueBuilder<>(key);
    }

    public static class ZaddValueBuilder<K, T> {

        private final Converter<T, K> key;

        public ZaddValueBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> ZaddBuilder<K, V, T> value(Converter<T, ScoredValue<V>> value) {
            return new ZaddBuilder<>(key, value);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class ZaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, ZaddBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, ScoredValue<V>> value;
        private ZAddArgs args;

        public ZaddBuilder(Converter<T, K> key, Converter<T, ScoredValue<V>> value) {
            super(value);
            this.key = key;
            this.value = value;
        }

        @Override
        public Zadd<K, V, T> build() {
            return new Zadd<>(key, del, remove, value, args);
        }

    }
}
