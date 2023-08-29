package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.Suggestion;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractOperation<K, V, T, Sugadd<K, V, T>> {

    private Function<T, Suggestion<V>> suggestion;

    private boolean incr;

    public Sugadd<K, V, T> suggestion(Function<T, Suggestion<V>> suggestion) {
        this.suggestion = suggestion;
        return this;
    }

    public Sugadd<K, V, T> incr(boolean incr) {
        this.incr = incr;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures) {
        futures.add(execute((RediSearchAsyncCommands<K, V>) commands, key(item), suggestion(item)));
    }

    private Suggestion<V> suggestion(T item) {
        return suggestion.apply(item);
    }

    private RedisFuture<Long> execute(RediSearchAsyncCommands<K, V> commands, K key, Suggestion<V> suggestion) {
        if (incr) {
            return commands.ftSugaddIncr(key, suggestion);
        }
        return commands.ftSugadd(key, suggestion);
    }

}
