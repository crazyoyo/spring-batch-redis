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
		if (map.isEmpty()) {
			return null;
		}
		if (hdel.test(item)) {
			return hashCommands.hdel(key, (K[]) map.keySet().toArray());
		}
		return hashCommands.hset(key, map);
	}

	public static <K, T> MapBuilder<K, T> key(Converter<T, K> key) {
		return new MapBuilder<>(key);
	}

	public static class MapBuilder<K, T> {

		private final Converter<T, K> key;

		public MapBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> map(Converter<T, Map<K, V>> map) {
			return new Builder<>(key, map);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Map<K, V>> map;
		private Predicate<T> hdel = t -> false;

		public Builder(Converter<T, K> key, Converter<T, Map<K, V>> map) {
			this.key = key;
			this.map = map;
			onNull(map);
		}

		public Builder<K, V, T> hdel(Predicate<T> hdel) {
			this.hdel = hdel;
			return this;
		}

		public Hset<K, V, T> build() {
			return new Hset<>(key, del, map, hdel);
		}
	}

}
