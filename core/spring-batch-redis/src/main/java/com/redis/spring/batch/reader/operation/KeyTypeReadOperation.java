package com.redis.spring.batch.reader.operation;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.reader.MappingRedisFuture;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyTypeReadOperation<K, V> implements Operation<K, V, K, KeyValue<K>> {

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<KeyValue<K>> execute(BaseRedisAsyncCommands<K, V> commands, K item) {
        RedisFuture<String> future = ((RedisKeyAsyncCommands<K, V>) commands).type(item);
        return new MappingRedisFuture<>(future.toCompletableFuture(), t -> keyType(item, t));
    }

    private KeyValue<K> keyType(K key, String type) {
        KeyValue<K> keyType = new KeyValue<>();
        keyType.setKey(key);
        keyType.setType(DataType.of(type));
        return keyType;
    }

}
