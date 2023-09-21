package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractOperation<K, V, T> {

    private Function<T, Map<K, V>> mapFunction;

    public void setMapFunction(Function<T, Map<K, V>> map) {
        this.mapFunction = map;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures) {
        Map<K, V> map = mapFunction.apply(item);
        if (!CollectionUtils.isEmpty(map)) {
            futures.add((RedisFuture) ((RedisHashAsyncCommands<K, V>) commands).hset(key, map));
        }
    }

}
