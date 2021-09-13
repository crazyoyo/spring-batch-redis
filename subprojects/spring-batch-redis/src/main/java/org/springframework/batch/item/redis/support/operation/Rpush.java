package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Rpush<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    private final Converter<T, V> member;

    public Rpush(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V> member) {
        super(key, delete, remove);
        this.member = member;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisListAsyncCommands<K, V>) commands).rpush(key, member.convert(item));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        return ((RedisListAsyncCommands<K, V>) commands).lrem(key, -1, member.convert(item));
    }

    public static <T> RpushMemberBuilder<String, T> key(String key) {
        return key(t -> key);
    }

    public static <K, T> RpushMemberBuilder<K, T> key(K key) {
        return key(t -> key);
    }

    public static <K, T> RpushMemberBuilder<K, T> key(Converter<T, K> key) {
        return new RpushMemberBuilder<>(key);
    }

    public static class RpushMemberBuilder<K, T> {

        private final Converter<T, K> key;

        public RpushMemberBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> RpushBuilder<K, V, T> member(Converter<T, V> member) {
            return new RpushBuilder<>(key, member);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class RpushBuilder<K, V, T> extends RemoveBuilder<K, V, T, RpushBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, V> member;

        public RpushBuilder(Converter<T, K> key, Converter<T, V> member) {
            super(member);
            this.key = key;
            this.member = member;
        }

        @Override
        public Rpush<K, V, T> build() {
            return new Rpush<>(key, del, remove, member);
        }

    }
}
