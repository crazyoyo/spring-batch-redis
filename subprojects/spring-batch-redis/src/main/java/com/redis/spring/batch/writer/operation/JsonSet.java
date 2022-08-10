package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, K> path;
	private final Converter<T, V> value;

	public JsonSet(Converter<T, K> key, Predicate<T> delPredicate, Converter<T, K> path, Converter<T, V> value) {
		super(key, delPredicate, new JsonDel<>(key, path));
		this.path = path;
		this.value = value;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path.convert(item), value.convert(item));
	}

	public static <K, T> PathJsonSetBuilder<K, T> key(Converter<T, K> key) {
		return new PathJsonSetBuilder<>(key);
	}

	public static class PathJsonSetBuilder<K, T> {

		private final Converter<T, K> key;

		public PathJsonSetBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public JsonSetValueBuilder<K, T> path(K path) {
			return new JsonSetValueBuilder<>(key, t -> path);
		}

		public JsonSetValueBuilder<K, T> path(Converter<T, K> path) {
			return new JsonSetValueBuilder<>(key, path);
		}
	}

	public static class JsonSetValueBuilder<K, T> {

		private final Converter<T, K> key;
		private final Converter<T, K> path;

		public JsonSetValueBuilder(Converter<T, K> key, Converter<T, K> path) {
			this.key = key;
			this.path = path;
		}

		public <V> JsonSetBuilder<K, V, T> value(Converter<T, V> value) {
			return new JsonSetBuilder<>(key, path, value);
		}
	}

	public static class JsonSetBuilder<K, V, T> extends DelBuilder<K, V, T, JsonSetBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, K> path;
		private final Converter<T, V> value;

		public JsonSetBuilder(Converter<T, K> key, Converter<T, K> path, Converter<T, V> value) {
			this.key = key;
			this.path = path;
			this.value = value;
			onNull(value);
		}

		public JsonSet<K, V, T> build() {
			return new JsonSet<>(key, del, path, value);
		}

	}
}
