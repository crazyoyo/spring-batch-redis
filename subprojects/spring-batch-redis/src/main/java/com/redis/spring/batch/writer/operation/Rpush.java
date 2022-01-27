package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class Rpush<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, V> memberConverter;

	public Rpush(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V> member) {
		super(key, delete, remove);
		this.memberConverter = member;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		V member = memberConverter.convert(item);
		if (member == null) {
			return null;
		}
		return ((RedisListAsyncCommands<K, V>) commands).rpush(key, member);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		V member = this.memberConverter.convert(item);
		if (member == null) {
			return null;
		}
		return ((RedisListAsyncCommands<K, V>) commands).lrem(key, -1, member);
	}

	public static <K, V, T> RpushMemberBuilder<K, V, T> key(K key) {
		return key(t -> key);
	}

	public static <K, V, T> RpushMemberBuilder<K, V, T> key(Converter<T, K> key) {
		return new RpushMemberBuilder<>(key);
	}

	public static class RpushMemberBuilder<K, V, T> {

		private final Converter<T, K> key;

		public RpushMemberBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public RpushBuilder<K, V, T> member(Converter<T, V> member) {
			return new RpushBuilder<>(key, member);
		}
	}

	public static class RpushBuilder<K, V, T> extends RemoveBuilder<K, V, T, RpushBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, V> member;

		public RpushBuilder(Converter<T, K> key, Converter<T, V> member) {
			super(member);
			this.key = key;
			this.member = member;
		}

		@Override
		public Rpush<K, V, T> build() {
			return new Rpush<>(key, del, remove, member);
		}

	}
}
