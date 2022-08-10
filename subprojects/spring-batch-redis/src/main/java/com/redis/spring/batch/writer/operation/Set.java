package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class Set<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, V> value;
	private final SetArgs args;

	public Set(Converter<T, K> key, Predicate<T> delete, Converter<T, V> value, SetArgs args) {
		super(key, delete);
		Assert.notNull(value, "A value converter is required");
		Assert.notNull(args, "A SetArgs converter is required");
		this.value = value;
		this.args = args;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<String> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisStringAsyncCommands<K, V>) commands).set(key, value.convert(item), args);
	}

	public static <K, T> ValueBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <K, T> ValueBuilder<K, T> key(Converter<T, K> key) {
		return new ValueBuilder<>(key);
	}

	public static class ValueBuilder<K, T> {

		private final Converter<T, K> key;

		public ValueBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> value(Converter<T, V> member) {
			return new Builder<>(key, member);
		}
	}

	public static class Builder<K, V, T> extends RemoveBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, V> member;
		private SetArgs args = new SetArgs();

		public Builder(Converter<T, K> key, Converter<T, V> member) {
			this.key = key;
			this.member = member;
			onNull(member);
		}

		public Builder<K, V, T> args(SetArgs args) {
			this.args = args;
			return this;
		}

		public Set<K, V, T> build() {
			return new Set<>(key, del, member, args);
		}

	}

}
