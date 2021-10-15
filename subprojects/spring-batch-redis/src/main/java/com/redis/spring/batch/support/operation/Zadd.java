package com.redis.spring.batch.support.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
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

	@Override
	protected RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		ScoredValue<V>[] scoredValues = values.convert(item);
		if (scoredValues == null) {
			return null;
		}
		return commands.zadd(key, args, scoredValues);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		ScoredValue<V>[] scoredValues = values.convert(item);
		if (scoredValues == null) {
			return null;
		}
		List<V> members = new ArrayList<>();
		for (ScoredValue<V> value : scoredValues) {
			members.add(value.getValue());
		}
		return commands.zrem(key, (V[]) members.toArray());
	}

	public static <T> ZaddValueBuilder<String, T> key(String key) {
		return key(t -> key);
	}

	public static <K, T> ZaddValueBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <K, T> ZaddValueBuilder<K, T> key(Converter<T, K> key) {
		return new ZaddValueBuilder<>(key);
	}

	public static class ZaddValueBuilder<K, T> {

		private final Converter<T, K> key;

		public ZaddValueBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> ZaddBuilder<K, V, T> values(Converter<T, ScoredValue<V>[]> values) {
			return new ZaddBuilder<>(key, values);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class ZaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, ZaddBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, ScoredValue<V>[]> values;
		private ZAddArgs args;

		public ZaddBuilder(Converter<T, K> key, Converter<T, ScoredValue<V>[]> values) {
			super(values);
			this.key = key;
			this.values = values;
		}

		@Override
		public Zadd<K, V, T> build() {
			return new Zadd<>(key, del, remove, values, args);
		}

	}
}
