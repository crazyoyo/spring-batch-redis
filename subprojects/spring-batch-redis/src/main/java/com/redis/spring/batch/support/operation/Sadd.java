package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;

public class Sadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, V> memberConverter;

	public Sadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V> member) {
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
		return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, member);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		V member = memberConverter.convert(item);
		if (member == null) {
			return null;
		}
		return ((RedisSetAsyncCommands<K, V>) commands).srem(key, member);
	}

	public static <K, V, T> SaddMemberBuilder<K, V, T> key(K key) {
		return key(t -> key);
	}

	public static <K, V, T> SaddMemberBuilder<K, V, T> key(Converter<T, K> key) {
		return new SaddMemberBuilder<>(key);
	}

	public static class SaddMemberBuilder<K, V, T> {

		private final Converter<T, K> key;

		public SaddMemberBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public SaddBuilder<K, V, T> member(Converter<T, V> member) {
			return new SaddBuilder<>(key, member);
		}
	}

	public static class SaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, SaddBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, V> member;

		public SaddBuilder(Converter<T, K> key, Converter<T, V> member) {
			super(member);
			this.key = key;
			this.member = member;
		}

		@Override
		public Sadd<K, V, T> build() {
			return new Sadd<>(key, del, remove, member);
		}

	}

}
