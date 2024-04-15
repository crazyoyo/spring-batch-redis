package com.redis.spring.batch.operation;

import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractKeyValueOperation<K, V, V, T> {

	public static final String ROOT_PATH = "$";

	private Function<T, String> pathFunction = t -> ROOT_PATH;

	public JsonSet(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setPath(String path) {
		this.pathFunction = t -> path;
	}

	public void setPathFunction(Function<T, String> path) {
		this.pathFunction = path;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, V value) {
		String path = pathFunction.apply(item);
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path, value);
	}

}
