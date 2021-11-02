package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.support.convert.ArrayConverter;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Lpush<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, V[]> memberArrayConverter;

	public Lpush(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V[]> members) {
		super(key, delete, remove);
		this.memberArrayConverter = members;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisListAsyncCommands<K, V>) commands).lpush(key, memberArrayConverter.convert(item));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		V[] members = this.memberArrayConverter.convert(item);
		if (members == null) {
			return null;
		}
		if (members.length > 1) {
			throw new RedisCommandExecutionException("Removal of multiple list members is not supported");
		}
		return ((RedisListAsyncCommands<K, V>) commands).lrem(key, 1, members[0]);
	}

	public static <T> LpushMemberBuilder<T> key(String key) {
		return key(t -> key);
	}

	public static <T> LpushMemberBuilder<T> key(Converter<T, String> key) {
		return new LpushMemberBuilder<>(key);
	}

	public static class LpushMemberBuilder<T> {

		private final Converter<T, String> key;

		public LpushMemberBuilder(Converter<T, String> key) {
			this.key = key;
		}

		@SuppressWarnings("unchecked")
		public LpushBuilder<T> members(Converter<T, String>... members) {
			return new LpushBuilder<>(key, new ArrayConverter<>(String.class, members));
		}

		public LpushBuilder<T> members(Converter<T, String[]> members) {
			return new LpushBuilder<>(key, members);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class LpushBuilder<T> extends RemoveBuilder<T, LpushBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, String[]> members;

		public LpushBuilder(Converter<T, String> key, Converter<T, String[]> members) {
			super(members);
			this.key = key;
			this.members = members;
		}

		@Override
		public Lpush<String, String, T> build() {
			return new Lpush<>(key, del, remove, members);
		}

	}
}
