package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyTypeReadOperation<K, V> implements Operation<K, V, K, KeyValue<K>> {

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<KeyValue<K>> execute(BaseRedisAsyncCommands<K, V> commands, K key) {
        RedisFuture<String> future = ((RedisKeyAsyncCommands<K, V>) commands).type(key);
        return new MappingRedisFuture<>(future.toCompletableFuture(), t -> keyValue(key, t));
    }

    private KeyValue<K> keyValue(K key, String type) {
        KeyValue<K> keyValue = new KeyValue<>();
        keyValue.setKey(key);
        keyValue.setType(DataType.of(type));
        return keyValue;
    }

}
