package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> implements Operation<K, V, T> {

    private final Function<T, K> keyFunction;

    private final Function<T, Collection<ScoredValue<V>>> membersFunction;

    private final ZAddArgs args;

    public ZaddAll(Function<T, K> key, Function<T, Collection<ScoredValue<V>>> members) {
        this(key, members, null);
    }

    public ZaddAll(Function<T, K> key, Function<T, Collection<ScoredValue<V>>> members, ZAddArgs args) {
        this.keyFunction = key;
        this.membersFunction = members;
        this.args = args;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        Collection<ScoredValue<V>> members = membersFunction.apply(item);
        if (members.isEmpty()) {
            return;
        }
        K key = keyFunction.apply(item);
        RedisSortedSetAsyncCommands<K, V> zset = (RedisSortedSetAsyncCommands<K, V>) commands;
        futures.add(zset.zadd(key, args, members.toArray(new ScoredValue[0])));
    }

}
