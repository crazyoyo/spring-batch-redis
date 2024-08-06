package com.redis.spring.batch.item.redis.writer.impl;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.item.redis.common.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractValueWriteOperation<K, V, V, T> {

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

	private String path(T item) {
		return pathFunction.apply(item);
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, List<? extends T> items) {
		return BatchUtils.executeAll(commands, items, this::execute);
	}

	private RedisFuture<?> execute(RedisAsyncCommands<K, V> commands, T item) {
		return ((RedisModulesAsyncCommands<K, V>) commands).jsonSet(key(item), path(item), value(item));
	}

}
