package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

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
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisStringAsyncCommands<K, V>) commands).set(key, value.convert(item), args);
	}

	public static <T> SetMemberBuilder<T> key(String key) {
		return key(t -> key);
	}

	public static <T> SetMemberBuilder<T> key(Converter<T, String> key) {
		return new SetMemberBuilder<>(key);
	}

	public static class SetMemberBuilder<T> {

		private final Converter<T, String> key;

		public SetMemberBuilder(Converter<T, String> key) {
			this.key = key;
		}

		public SetBuilder<T> value(Converter<T, String> member) {
			return new SetBuilder<>(key, member);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class SetBuilder<T> extends RemoveBuilder<T, SetBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, String> member;
		private SetArgs args = new SetArgs();

		public SetBuilder(Converter<T, String> key, Converter<T, String> member) {
			super(member);
			this.key = key;
			this.member = member;
		}

		@Override
		public Set<String, String, T> build() {
			return new Set<>(key, del, member, args);
		}

	}

}
