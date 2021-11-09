package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.GeoAddArgs;
import io.lettuce.core.GeoValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class Geoadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, GeoValue<V>> valueConverter;
	private final GeoAddArgs args;

	public Geoadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, GeoValue<V>> value,
			GeoAddArgs args) {
		super(key, delete, remove);
		Assert.notNull(value, "A geo-value converter is required");
		this.valueConverter = value;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		GeoValue<V> value = valueConverter.convert(item);
		if (value == null) {
			return null;
		}
		return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, args, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		GeoValue<V> value = this.valueConverter.convert(item);
		if (value == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, value.getValue());
	}

	public static <K, V, T> GeoaddValueBuilder<K, V, T> key(K key) {
		return key(t -> key);
	}

	public static <K, V, T> GeoaddValueBuilder<K, V, T> key(Converter<T, K> key) {
		return new GeoaddValueBuilder<>(key);
	}

	public static class GeoaddValueBuilder<K, V, T> {

		private final Converter<T, K> key;

		public GeoaddValueBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public GeoaddBuilder<K, V, T> value(Converter<T, GeoValue<V>> value) {
			return new GeoaddBuilder<>(key, value);
		}
	}

	public static class GeoaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, GeoaddBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, GeoValue<V>> value;
		private GeoAddArgs args;

		public GeoaddBuilder(Converter<T, K> key, Converter<T, GeoValue<V>> value) {
			super(value);
			this.key = key;
			this.value = value;
		}

		public GeoaddBuilder<K, V, T> args(GeoAddArgs args) {
			this.args = args;
			return this;
		}

		@Override
		public Geoadd<K, V, T> build() {
			return new Geoadd<>(key, del, remove, value, args);
		}

	}

}
