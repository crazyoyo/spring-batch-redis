package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.search.Suggestion;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.function.Predicate;

public class Sugadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

    protected final Converter<T, Suggestion<V>> suggestion;
    private final boolean incr;

    public Sugadd(Converter<T, K> key, Predicate<T> remove, Converter<T, Suggestion<V>> suggestion, boolean incr) {
        super(key, t -> false, remove);
        Assert.notNull(suggestion, "A suggestion converter is required");
        this.suggestion = suggestion;
        this.incr = incr;
    }

    @Override
    protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Suggestion<V> suggestion = this.suggestion.convert(item);
        if (incr) {
            return ((RedisModulesAsyncCommands<K, V>) commands).sugaddIncr(key, suggestion);
        }
        return ((RedisModulesAsyncCommands<K, V>) commands).sugadd(key, suggestion);
    }

    @Override
    protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Suggestion<V> suggestion = this.suggestion.convert(item);
        if (suggestion == null) {
            return null;
        }
        return ((RedisModulesAsyncCommands<K, V>) commands).sugdel(key, suggestion.getString());
    }

    public static <T> SugaddSuggestionBuilder<String, T> key(String key) {
        return new SugaddSuggestionBuilder<>(t -> key);
    }

    public static <K, T> SugaddSuggestionBuilder<K, T> key(K key) {
        return new SugaddSuggestionBuilder<>(t -> key);
    }

    public static <K, T> SugaddSuggestionBuilder<K, T> key(Converter<T, K> key) {
        return new SugaddSuggestionBuilder<>(key);
    }

    public static class SugaddSuggestionBuilder<K, T> {

        private final Converter<T, K> key;

        public SugaddSuggestionBuilder(Converter<T, K> key) {
            this.key = key;
        }

        public <V> SugaddBuilder<K, V, T> suggestion(Converter<T, Suggestion<V>> suggestion) {
            return new SugaddBuilder<>(key, suggestion);
        }
    }

    @Setter
    @Accessors(fluent = true)
    public static class SugaddBuilder<K, V, T> extends DelBuilder<K, V, T, SugaddBuilder<K, V, T>> {

        private final Converter<T, K> key;
        private final Converter<T, Suggestion<V>> suggestion;
        private boolean increment;

        public SugaddBuilder(Converter<T, K> key, Converter<T, Suggestion<V>> suggestion) {
            super(suggestion);
            this.key = key;
            this.suggestion = suggestion;
        }

        @Override
        public Sugadd<K, V, T> build() {
            return new Sugadd<>(key, del, suggestion, increment);
        }
    }

}
