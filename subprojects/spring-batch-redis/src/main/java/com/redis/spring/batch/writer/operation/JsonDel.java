package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonDel<K, V, T> extends AbstractWriteOperation<K, V, T> {

	private final Function<T, String> path;

	public JsonDel(Function<T, K> key) {
		this(key, JsonSet.rootPath());
	}

	public JsonDel(Function<T, K> key, Function<T, String> path) {
		super(key);
		this.path = path;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key, path.apply(item));
	}

}
