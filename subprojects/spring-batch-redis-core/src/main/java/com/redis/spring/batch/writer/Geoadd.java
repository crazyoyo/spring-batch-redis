package com.redis.spring.batch.writer;

import java.util.function.Function;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

public class Geoadd<K, V, T> extends AbstractKeyValueOperation<K, V, GeoValue<V>, T> {

	private Function<T, GeoAddArgs> argsFunction = t -> null;

	public Geoadd(Function<T, K> keyFunction, Function<T, GeoValue<V>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setArgs(GeoAddArgs args) {
		this.argsFunction = t -> args;
	}

	public void setArgsFunction(Function<T, GeoAddArgs> args) {
		this.argsFunction = args;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, GeoValue<V> value) {
		return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, argsFunction.apply(item), value);
	}

}
