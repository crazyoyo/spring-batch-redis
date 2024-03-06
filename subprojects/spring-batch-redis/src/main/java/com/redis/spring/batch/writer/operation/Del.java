package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Del<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	public Del(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		outputs.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(key));
	}

}
