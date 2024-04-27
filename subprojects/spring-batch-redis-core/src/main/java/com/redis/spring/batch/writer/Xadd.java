package com.redis.spring.batch.writer;

import java.util.Map;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;

public class Xadd<K, V, T> extends AbstractKeyValueOperation<K, V, Map<K, V>, T> {

	private Function<T, XAddArgs> argsFunction = t -> null;

	public Xadd(Function<T, K> keyFunction, Function<T, Map<K, V>> bodyFunction) {
		super(keyFunction, bodyFunction, CollectionUtils::isEmpty);
	}

	public void setArgs(XAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, XAddArgs> function) {
		this.argsFunction = function;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Map<K, V> value) {
		return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, argsFunction.apply(item), value);
	}

}
