package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

public class Geoadd<K, V, T> extends AbstractWriteOperation<K, V, T> {

	private final Function<T, GeoValue<V>> value;
	private final Function<T, GeoAddArgs> args;

	public Geoadd(Function<T, K> key, Function<T, GeoValue<V>> value) {
		this(key, value, t -> null);
	}

	public Geoadd(Function<T, K> key, Function<T, GeoValue<V>> value, Function<T, GeoAddArgs> args) {
		super(key);
		this.value = value;
		this.args = args;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, args.apply(item), value.apply(item));
	}

}
