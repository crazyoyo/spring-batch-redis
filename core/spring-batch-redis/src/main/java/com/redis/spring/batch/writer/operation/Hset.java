package com.redis.spring.batch.writer.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, Map<K, V>> mapFunction;

	public Hset(Function<T, K> keyFunction, Function<T, Map<K, V>> mapFunction) {
		super(keyFunction);
		this.mapFunction = mapFunction;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		Map<K, V> map = mapFunction.apply(item);
		if (!CollectionUtils.isEmpty(map)) {
			outputs.add((RedisFuture) ((RedisHashAsyncCommands<K, V>) commands).hset(key, map));
		}
	}

}
