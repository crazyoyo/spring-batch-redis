package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractOperation<K, V, T> {

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
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		futures.add(((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path.apply(item), value.apply(item)));
	}

	public static <T> Function<T, String> rootPath() {
		return t -> "$";
	}
}
