package com.redis.spring.batch.writer.operation;

import java.util.Map;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;

public class Hset<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, Map<K, V>> mapConverter;
	private final Predicate<T> hdel;

	public Hset(Converter<T, K> key, Predicate<T> delete, Converter<T, Map<K, V>> map, Predicate<T> hdel) {
		super(key, delete);
		Assert.notNull(map, "A map converter is required");
		this.mapConverter = map;
		this.hdel = hdel;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		RedisHashAsyncCommands<K, V> hashCommands = ((RedisHashAsyncCommands<K, V>) commands);
		Map<K, V> map = mapConverter.convert(item);
		if (hdel.test(item)) {
			return hashCommands.hdel(key, (K[]) map.keySet().toArray());
		}
		return hashCommands.hset(key, map);
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
		private Predicate<T> hdel = t -> false;

		public HsetBuilder(Converter<T, K> key, Converter<T, Map<K, V>> map) {
			super(map);
			this.key = key;
			this.map = map;
		}

		public HsetBuilder<K, V, T> hdel(Predicate<T> hdel) {
			this.hdel = hdel;
			return this;
		}

		@Override
		public Hset<K, V, T> build() {
			return new Hset<>(key, del, map, hdel);
		}
	}

}
