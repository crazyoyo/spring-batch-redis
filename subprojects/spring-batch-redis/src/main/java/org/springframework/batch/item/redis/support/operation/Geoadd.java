package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Geoadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    private final Converter<T, GeoValue<V>> value;
    private final GeoAddArgs args;

    public Geoadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, GeoValue<V>> value, GeoAddArgs args) {
        super(key, delete, remove);
        Assert.notNull(value, "A geo-value converter is required");
        this.value = value;
        this.args = args;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        return commands.geoadd(key, args, value.convert(item));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        GeoValue<V> value = this.value.convert(item);
        if (value == null) {
            return null;
        }
        return commands.zrem(key, value.getValue());
    }

    public static <T> GeoaddValueBuilder<String, T> key(String key) {
        return key(t -> key);
    }

    public static <K, T> GeoaddValueBuilder<K, T> key(K key) {
        return key(t -> key);
    }

    public static <K, T> GeoaddValueBuilder<K, T> key(Converter<T, K> key) {
        return new GeoaddValueBuilder<>(key);
    }

    public static class GeoaddValueBuilder<K, T> {

        private final Converter<T, K> key;

        public GeoaddValueBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> GeoaddBuilder<K, V, T> value(Converter<T, GeoValue<V>> value) {
            return new GeoaddBuilder<>(key, value);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class GeoaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, GeoaddBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, GeoValue<V>> value;
        private GeoAddArgs args;

        public GeoaddBuilder(Converter<T, K> key, Converter<T, GeoValue<V>> value) {
            super(value);
            this.key = key;
            this.value = value;
        }

        @Override
        public Geoadd<K, V, T> build() {
            return new Geoadd<>(key, del, remove, value, args);
        }

    }

}
