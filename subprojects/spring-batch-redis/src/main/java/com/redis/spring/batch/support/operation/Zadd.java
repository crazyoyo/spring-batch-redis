package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class Zadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, ScoredValue<V>> value;
	private final ZAddArgs args;

	public Zadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, ScoredValue<V>> value,
			ZAddArgs args) {
		super(key, delete, remove);
		Assert.notNull(value, "A scored value converter is required");
		this.value = value;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		ScoredValue<V> scoredValue = value.convert(item);
		if (scoredValue == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, scoredValue);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		ScoredValue<V> scoredValue = value.convert(item);
		if (scoredValue == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, scoredValue.getValue());
	}

	public static <K, V, T> ZaddValueBuilder<K, V, T> key(K key) {
		return key(t -> key);
	}

	public static <K, V, T> ZaddValueBuilder<K, V, T> key(Converter<T, K> key) {
		return new ZaddValueBuilder<>(key);
	}

	public static class ZaddValueBuilder<K, V, T> {

		private final Converter<T, K> key;

		public ZaddValueBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public ZaddBuilder<K, V, T> value(Converter<T, ScoredValue<V>> value) {
			return new ZaddBuilder<>(key, value);
		}
	}

	public static class ZaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, ZaddBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, ScoredValue<V>> value;
		private ZAddArgs args;

		public ZaddBuilder(Converter<T, K> key, Converter<T, ScoredValue<V>> value) {
			super(value);
			this.key = key;
			this.value = value;
		}

		public ZaddBuilder<K, V, T> args(ZAddArgs args) {
			this.args = args;
			return this;
		}

		@Override
		public Zadd<K, V, T> build() {
			return new Zadd<>(key, del, remove, value, args);
		}

	}
}
