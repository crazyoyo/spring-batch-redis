package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.List;
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

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key,
			Collection<GeoValue<V>> values) {
		futures.add(((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, args, values.toArray(new GeoValue[0])));
	}

}
