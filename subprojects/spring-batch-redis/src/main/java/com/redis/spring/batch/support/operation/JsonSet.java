package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, K> path;
	private final Converter<T, V> value;

	public JsonSet(Converter<T, K> key, Predicate<T> delete, Converter<T, K> path, Converter<T, V> value) {
		super(key, delete);
		this.path = path;
		this.value = value;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path.convert(item), value.convert(item));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> delete(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonDel(key, path.convert(item));
	}

	public static <T> PathJsonSetBuilder<T> key(Converter<T, String> key) {
		return new PathJsonSetBuilder<>(key);
	}

	public static class PathJsonSetBuilder<T> {

		private final Converter<T, String> key;

		public PathJsonSetBuilder(Converter<T, String> key) {
			this.key = key;
		}

		public JsonSetValueBuilder<T> path(String path) {
			return new JsonSetValueBuilder<>(key, t -> path);
		}

		public JsonSetValueBuilder<T> path(Converter<T, String> path) {
			return new JsonSetValueBuilder<>(key, path);
		}
	}

	public static class JsonSetValueBuilder<T> {

		private final Converter<T, String> key;
		private final Converter<T, String> path;

		public JsonSetValueBuilder(Converter<T, String> key, Converter<T, String> path) {
			this.key = key;
			this.path = path;
		}

		public <V> JsonSetBuilder<T> value(Converter<T, String> value) {
			return new JsonSetBuilder<>(key, path, value);
		}
	}

	public static class JsonSetBuilder<T> extends DelBuilder<T, JsonSetBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, String> path;
		private final Converter<T, String> value;

		public JsonSetBuilder(Converter<T, String> key, Converter<T, String> path, Converter<T, String> value) {
			super(value);
			this.key = key;
			this.path = path;
			this.value = value;
		}

		@Override
		public JsonSet<String, String, T> build() {
			return new JsonSet<>(key, del, path, value);
		}

	}
}
