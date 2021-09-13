package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.function.Predicate;

public class Hset<K, V, T> extends AbstractKeyOperation<K, V, T> {

    private final Converter<T, Map<K, V>> map;

    public Hset(Converter<T, K> key, Predicate<T> delete, Converter<T, Map<K, V>> map) {
        super(key, delete);
        Assert.notNull(map, "A map converter is required");
        this.map = map;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisHashAsyncCommands<K, V>) commands).hset(key, map.convert(item));
    }

    public static <K, T> HsetMapBuilder<K, T> key(Converter<T, K> key) {
        return new HsetMapBuilder<>(key);
    }

    public static class HsetMapBuilder<K, T> {

        private final Converter<T, K> key;

        public HsetMapBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> HsetBuilder<K, V, T> map(Converter<T, Map<K, V>> map) {
            return new HsetBuilder<>(key, map);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class HsetBuilder<K, V, T> extends DelBuilder<K, V, T, HsetBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, Map<K, V>> map;

        public HsetBuilder(Converter<T, K> key, Converter<T, Map<K, V>> map) {
            super(map);
            this.key = key;
            this.map = map;
        }

        @Override
        public Hset<K, V, T> build() {
            return new Hset<>(key, del, map);
        }
    }

}

