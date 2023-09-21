package com.redis.spring.batch.writer.operation;

import java.util.function.Function;
import java.util.function.Predicate;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractSingleOperation<K, V, T> {

    private Function<T, Suggestion<V>> suggestionFunction;

    private Predicate<T> incrPredicate = Predicates.isFalse();

    public void setSuggestionFunction(Function<T, Suggestion<V>> function) {
        this.suggestionFunction = function;
    }

    public void setIncr(boolean incr) {
        this.incrPredicate = t -> incr;
    }

    public void setIncrPredicate(Predicate<T> predicate) {
        this.incrPredicate = predicate;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        RediSearchAsyncCommands<K, V> searchCommands = (RediSearchAsyncCommands<K, V>) commands;
        Suggestion<V> suggestion = suggestionFunction.apply(item);
        if (incrPredicate.test(item)) {
            return searchCommands.ftSugaddIncr(key, suggestion);
        }
        return searchCommands.ftSugadd(key, suggestion);
    }

}
