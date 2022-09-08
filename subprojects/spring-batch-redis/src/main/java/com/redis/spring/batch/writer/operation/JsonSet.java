package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class JsonSet<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, String> path;
	private final Converter<T, V> value;

	public JsonSet(Converter<T, K> key, Predicate<T> delPredicate, Converter<T, String> path, Converter<T, V> value) {
		super(key, delPredicate, new JsonDel<>(key, path));
		this.path = path;
		this.value = value;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisJSONAsyncCommands<K, V>) commands).jsonSet(key, path.convert(item), value.convert(item));
	}

	public static <K, T> ValueBuilder<K, T> key(Converter<T, K> key) {
		return new ValueBuilder<>(key);
	}

	public static class ValueBuilder<K, T> {

		private final Converter<T, K> key;

		public ValueBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> value(Converter<T, V> value) {
			return new Builder<>(key, value);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private Converter<T, String> path = t -> "$";
		private final Converter<T, V> value;

		public Builder(Converter<T, K> key, Converter<T, V> value) {
			this.key = key;
			this.value = value;
			onNull(value);
		}

		public Builder<K, V, T> path(String path) {
			this.path = t -> path;
			return this;
		}

		public Builder<K, V, T> path(Converter<T, String> path) {
			this.path = path;
			return this;
		}

		public JsonSet<K, V, T> build() {
			return new JsonSet<>(key, del, path, value);
		}

	}
}
