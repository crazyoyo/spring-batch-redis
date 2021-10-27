package com.redis.spring.batch.support.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.spring.batch.support.convert.ArrayConverter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Zadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, ScoredValue<V>[]> values;
	private final ZAddArgs args;

	public Zadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, ScoredValue<V>[]> values,
			ZAddArgs args) {
		super(key, delete, remove);
		Assert.notNull(values, "A scored value converter is required");
		this.values = values;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		ScoredValue<V>[] scoredValues = values.convert(item);
		if (scoredValues == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, args, scoredValues);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		ScoredValue<V>[] scoredValues = values.convert(item);
		if (scoredValues == null) {
			return null;
		}
		List<V> members = new ArrayList<>();
		for (ScoredValue<V> value : scoredValues) {
			members.add(value.getValue());
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zrem(key, (V[]) members.toArray());
	}

	public static <T> ZaddValueBuilder<T> key(String key) {
		return key(t -> key);
	}

	public static <T> ZaddValueBuilder<T> key(Converter<T, String> key) {
		return new ZaddValueBuilder<>(key);
	}

	public static class ZaddValueBuilder<T> {

		private final Converter<T, String> key;

		public ZaddValueBuilder(Converter<T, String> key) {
			this.key = key;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public ZaddBuilder<T> values(Converter<T, ScoredValue<String>>... values) {
			return new ZaddBuilder<>(key,
					new ArrayConverter<T, ScoredValue<String>>((Class) ScoredValue.class, values));
		}

		public ZaddBuilder<T> values(Converter<T, ScoredValue<String>[]> values) {
			return new ZaddBuilder<>(key, values);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class ZaddBuilder<T> extends RemoveBuilder<T, ZaddBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, ScoredValue<String>[]> values;
		private ZAddArgs args;

		public ZaddBuilder(Converter<T, String> key, Converter<T, ScoredValue<String>[]> values) {
			super(values);
			this.key = key;
			this.values = values;
		}

		@Override
		public Zadd<String, String, T> build() {
			return new Zadd<>(key, del, remove, values, args);
		}

	}
}
