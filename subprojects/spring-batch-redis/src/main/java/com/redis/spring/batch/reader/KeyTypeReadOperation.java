package com.redis.spring.batch.reader;

import org.springframework.batch.item.Chunk;

import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyTypeReadOperation<K, V> implements Operation<K, V, K, KeyValue<K>> {

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends K> inputs,
			Chunk<RedisFuture<KeyValue<K>>> outputs) {
		RedisKeyAsyncCommands<K, V> keyCommands = (RedisKeyAsyncCommands<K, V>) commands;
		for (K key : inputs) {
			RedisFuture<String> future = keyCommands.type(key);
			outputs.add(new MappingRedisFuture<>(future.toCompletableFuture(), t -> keyValue(key, t)));
		}
	}

	private KeyValue<K> keyValue(K key, String type) {
		KeyValue<K> keyValue = new KeyValue<>();
		keyValue.setKey(key);
		keyValue.setType(DataType.of(type));
		return keyValue;
	}

}
