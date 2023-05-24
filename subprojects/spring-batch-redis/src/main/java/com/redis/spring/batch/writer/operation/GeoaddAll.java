package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Function;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;

public class GeoaddAll<K, V, T> extends AbstractAddAllOperation<K, V, T, GeoValue<V>> {

	private final GeoAddArgs args;

	public GeoaddAll(Function<T, K> key, Function<T, Collection<GeoValue<V>>> value) {
		this(key, value, null);
	}

	public GeoaddAll(Function<T, K> key, Function<T, Collection<GeoValue<V>>> value, GeoAddArgs args) {
		super(key, value);
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key,
			Collection<GeoValue<V>> values) {
		return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, args, values.toArray(new GeoValue[0]));
	}

}
