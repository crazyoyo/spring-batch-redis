package com.redis.spring.batch.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonDel<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private Function<T, String> pathFunction = t -> JsonSet.ROOT_PATH;

	public JsonDel(Function<T, K> keyFunction) {
		super(keyFunction);
	}

	public void setPath(String path) {
		this.pathFunction = t -> path;
	}

	public void setPathFunction(Function<T, String> path) {
		this.pathFunction = path;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> outputs) {
		String path = pathFunction.apply(item);
		outputs.add((RedisFuture) ((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key, path));
	}

}
