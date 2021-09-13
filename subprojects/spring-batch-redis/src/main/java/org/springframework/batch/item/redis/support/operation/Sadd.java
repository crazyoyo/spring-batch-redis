package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Sadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    private final Converter<T, V> member;

    public Sadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V> member) {
        super(key, delete, remove);
        this.member = member;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, member.convert(item));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisSetAsyncCommands<K, V>) commands).srem(key, member.convert(item));
    }

    public static <T> SaddMemberBuilder<String, T> key(String key) {
        return key(t -> key);
    }

    public static <K, T> SaddMemberBuilder<K, T> key(K key) {
        return key(t -> key);
    }

    public static <K, T> SaddMemberBuilder<K, T> key(Converter<T, K> key) {
        return new SaddMemberBuilder<>(key);
    }

    public static class SaddMemberBuilder<K, T> {

        private final Converter<T, K> key;

        public SaddMemberBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> SaddBuilder<K, V, T> member(Converter<T, V> member) {
            return new SaddBuilder<>(key, member);
        }
    }

    public static class SaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, Rpush.RpushBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, V> member;

        public SaddBuilder(Converter<T, K> key, Converter<T, V> member) {
            super(member);
            this.key = key;
            this.member = member;
        }

        @Override
        public Sadd<K, V, T> build() {
            return new Sadd<>(key, del, remove, member);
        }

    }

}

