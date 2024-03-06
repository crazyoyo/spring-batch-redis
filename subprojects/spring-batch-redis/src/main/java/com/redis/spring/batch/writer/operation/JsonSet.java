package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	public static final String ROOT_PATH = "$";

	private final Function<T, V> valueFunction;
	private Function<T, String> pathFunction = t -> ROOT_PATH;

	public JsonSet(Function<T, K> keyFunction, Function<T, V> valueFunction) {
		super(keyFunction);
		this.valueFunction = valueFunction;
	}

	public void setPath(String path) {
		this.pathFunction = t -> path;
	}

	public void setPathFunction(Function<T, String> path) {
		this.pathFunction = path;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		String path = pathFunction.apply(item);
		V value = valueFunction.apply(item);
		outputs.add((RedisFuture) ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path, value));
	}

}
