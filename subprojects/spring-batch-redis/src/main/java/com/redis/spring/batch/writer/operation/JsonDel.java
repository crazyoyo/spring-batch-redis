package com.redis.spring.batch.writer.operation;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonDel<K, V, T> implements Operation<K, V, T> {

	private final Converter<T, K> key;
	private final Converter<T, String> path;

	public JsonDel(Converter<T, K> key, Converter<T, String> path) {
		this.key = key;
		this.path = path;
	}

	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key.convert(item), path.convert(item));
	}

}
