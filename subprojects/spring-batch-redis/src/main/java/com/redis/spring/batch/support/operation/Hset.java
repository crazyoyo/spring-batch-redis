package com.redis.spring.batch.support.operation;

import java.util.Map;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Hset<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, Map<K, V>> map;

	public Hset(Converter<T, K> key, Predicate<T> delete, Converter<T, Map<K, V>> map) {
		super(key, delete);
		Assert.notNull(map, "A map converter is required");
		this.map = map;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisHashAsyncCommands<K, V>) commands).hset(key, map.convert(item));
	}

	public static <T> HsetMapBuilder<T> key(Converter<T, String> key) {
		return new HsetMapBuilder<>(key);
	}

	public static class HsetMapBuilder<T> {

		private final Converter<T, String> key;

		public HsetMapBuilder(Converter<T, String> key) {
			this.key = key;
		}

		public HsetBuilder<T> map(Converter<T, Map<String, String>> map) {
			return new HsetBuilder<>(key, map);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class HsetBuilder<T> extends DelBuilder<T, HsetBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, Map<String, String>> map;

		public HsetBuilder(Converter<T, String> key, Converter<T, Map<String, String>> map) {
			super(map);
			this.key = key;
			this.map = map;
		}

		@Override
		public Hset<String, String, T> build() {
			return new Hset<>(key, del, map);
		}
	}

}
