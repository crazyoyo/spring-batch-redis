package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

public class Geoadd<K, V, T> extends AbstractOperation<K, V, T> {

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
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		futures.add(((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, args.apply(item), value.apply(item)));
	}

}
