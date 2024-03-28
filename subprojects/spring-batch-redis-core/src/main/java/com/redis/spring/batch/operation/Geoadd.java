package com.redis.spring.batch.operation;

import java.util.function.Function;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

public class Geoadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, GeoValue<V>> valueFunction;
	private Function<T, GeoAddArgs> argsFunction = t -> null;

	public Geoadd(Function<T, K> keyFunction, Function<T, GeoValue<V>> valueFunction) {
		super(keyFunction);
		this.valueFunction = valueFunction;
	}

	public void setArgs(GeoAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, GeoAddArgs> args) {
		this.argsFunction = args;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		GeoAddArgs args = argsFunction.apply(item);
		GeoValue<V> value = valueFunction.apply(item);
		outputs.add((RedisFuture) ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, args, value));
	}

}
