package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyTypeReadOperation<K, V> implements Operation<K, V, K, KeyValue<K>> {

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<KeyValue<K>> execute(BaseRedisAsyncCommands<K, V> commands, K item) {
        RedisFuture<String> future = ((RedisKeyAsyncCommands<K, V>) commands).type(item);
        return new MappingRedisFuture<>(future.toCompletableFuture(), t -> KeyValue.key(item).type(t).build());
    }

}
