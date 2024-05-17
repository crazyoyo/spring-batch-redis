package com.redis.spring.batch.item.redis.writer.operation;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.writer.AbstractWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class JsonDel<K, V, T> extends AbstractWriteOperation<K, V, T> {

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

	private String path(T item) {
		return pathFunction.apply(item);
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		return BatchUtils.executeAll(commands, items, this::execute);
	}

	private RedisFuture<Long> execute(RedisAsyncCommands<K, V> commands, T item) {
		return ((RedisModulesAsyncCommands<K, V>) commands).jsonDel(key(item), path(item));
	}

}
