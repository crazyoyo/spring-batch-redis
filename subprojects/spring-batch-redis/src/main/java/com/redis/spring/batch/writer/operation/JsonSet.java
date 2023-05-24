package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractWriteOperation<K, V, T, String> {

	private final Function<T, String> path;
	private final Function<T, V> value;

	public JsonSet(Function<T, K> key, Function<T, V> value) {
		this(key, value, rootPath());
	}

	public JsonSet(Function<T, K> key, Function<T, V> value, Function<T, String> path) {
		super(key);
		this.path = path;
		this.value = value;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<String> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path.apply(item), value.apply(item));
	}

	public static <T> Function<T, String> rootPath() {
		return t -> "$";
	}
}
