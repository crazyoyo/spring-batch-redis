package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.Suggestion;

import io.lettuce.core.RedisFuture;

public class SugaddIncr<K, V, T> extends Sugadd<K, V, T> {

    public SugaddIncr(Function<T, K> key, Function<T, Suggestion<V>> suggestion) {
        super(key, suggestion);
    }

    @Override
    protected RedisFuture<Long> execute(RediSearchAsyncCommands<K, V> commands, K key, Suggestion<V> suggestion) {
        return commands.ftSugaddIncr(key, suggestion);
    }

}
