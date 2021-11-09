package com.redis.spring.batch.support.operation;

import java.util.Map;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, Map<K, V>> map;

	public Hset(Converter<T, K> key, Predicate<T> delete, Converter<T, Map<K, V>> map) {
		super(key, delete);
		Assert.notNull(map, "A map converter is required");
		this.map = map;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisHashAsyncCommands<K, V>) commands).hset(key, map.convert(item));
	}

	public static <K, V, T> HsetMapBuilder<K, V, T> key(Converter<T, K> key) {
		return new HsetMapBuilder<>(key);
	}

	public static class HsetMapBuilder<K, V, T> {

		private final Converter<T, K> key;

		public HsetMapBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public HsetBuilder<K, V, T> map(Converter<T, Map<K, V>> map) {
			return new HsetBuilder<>(key, map);
		}
	}

	public static class HsetBuilder<K, V, T> extends DelBuilder<K, V, T, HsetBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Map<K, V>> map;

		public HsetBuilder(Converter<T, K> key, Converter<T, Map<K, V>> map) {
			super(map);
			this.key = key;
			this.map = map;
		}

		@Override
		public Hset<K, V, T> build() {
			return new Hset<>(key, del, map);
		}
	}

}
