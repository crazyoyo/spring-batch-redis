package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonDel<K, V, T> implements WriteOperation<K, V, T> {

	private final Function<T, K> key;
	private final Function<T, String> path;

	public JsonDel(Function<T, K> key) {
		this(key, JsonSet.rootPath());
	}

	public JsonDel(Function<T, K> key, Function<T, String> path) {
		this.key = key;
		this.path = path;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item) {
		futures.add(((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key.apply(item), path.apply(item)));
	}

}
