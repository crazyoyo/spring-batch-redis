package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private Function<T, V> valueFunction;

	public Sadd(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction);
		this.valueFunction = valueFunction;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		V value = valueFunction.apply(item);
		outputs.add((RedisFuture) ((RedisSetAsyncCommands<K, V>) commands).sadd(key, value));
	}

}
