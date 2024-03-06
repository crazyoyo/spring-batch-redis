package com.redis.spring.batch.writer.operation;

import java.util.Map;
import java.util.function.Function;

import org.springframework.batch.item.Chunk;
import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, Map<K, V>> bodyFunction;
	private Function<T, XAddArgs> argsFunction = t -> null;

	public Xadd(Function<T, K> keyFunction, Function<T, Map<K, V>> bodyFunction) {
		super(keyFunction);
		this.bodyFunction = bodyFunction;
	}

	public void setArgs(XAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, XAddArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		Map<K, V> map = bodyFunction.apply(item);
		if (!CollectionUtils.isEmpty(map)) {
			XAddArgs args = argsFunction.apply(item);
			outputs.add((RedisFuture) ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args, map));
		}
	}

}
