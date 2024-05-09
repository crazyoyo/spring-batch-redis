package com.redis.spring.batch.item.redis.writer;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Del<K, V, T> extends AbstractKeyOperation<K, V, T> {

	public Del(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> outputs) {
		outputs.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(key));
	}

}
