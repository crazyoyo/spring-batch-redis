package com.redis.spring.batch.reader;

import java.util.List;

import com.redis.spring.batch.common.DataStructureType;
import com.redis.spring.batch.common.KeyType;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyTypeReadOperation<K, V> implements Operation<K, V, K, KeyType<K>> {

    @SuppressWarnings("unchecked")
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, K item, List<RedisFuture<KeyType<K>>> futures) {
        RedisFuture<String> future = ((RedisKeyAsyncCommands<K, V>) commands).type(item);
        futures.add(new MappingRedisFuture<>(future.toCompletableFuture(), t -> keyType(item, t)));
    }

    private KeyType<K> keyType(K key, String type) {
        KeyType<K> keyType = new KeyType<>();
        keyType.setKey(key);
        keyType.setType(DataStructureType.of(type));
        return keyType;
    }

}
