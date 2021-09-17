package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Lpush<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    private final Converter<T, V> member;

    public Lpush(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V> member) {
        super(key, delete, remove);
        this.member = member;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        return commands.lpush(key, member.convert(item));
    }

    @Override
    protected RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
        return commands.lrem(key, 1, member.convert(item));
    }

    public static <T> LpushMemberBuilder<String, T> key(String key) {
        return key(t -> key);
    }

    public static <K, T> LpushMemberBuilder<K, T> key(K key) {
        return key(t -> key);
    }

    public static <K, T> LpushMemberBuilder<K, T> key(Converter<T, K> key) {
        return new LpushMemberBuilder<>(key);
    }

    public static class LpushMemberBuilder<K, T> {

        private final Converter<T, K> key;

        public LpushMemberBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> LpushBuilder<K, V, T> member(Converter<T, V> member) {
            return new LpushBuilder<>(key, member);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class LpushBuilder<K, V, T> extends RemoveBuilder<K, V, T, LpushBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, V> member;

        public LpushBuilder(Converter<T, K> key, Converter<T, V> member) {
            super(member);
            this.key = key;
            this.member = member;
        }

        @Override
        public Lpush<K, V, T> build() {
            return new Lpush<>(key, del, remove, member);
        }

    }
}
