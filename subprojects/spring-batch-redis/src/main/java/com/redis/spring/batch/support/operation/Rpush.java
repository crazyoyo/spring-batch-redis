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

public class Rpush<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, V[]> members;

	public Rpush(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V[]> members) {
		super(key, delete, remove);
		this.members = members;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return ((RedisListAsyncCommands<K, V>) commands).rpush(key, members.convert(item));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		V[] members = this.members.convert(item);
		if (members == null) {
			return null;
		}
		if (members.length > 1) {
			throw new RedisCommandExecutionException("Removal of multiple list members is not supported");
		}
		return ((RedisListAsyncCommands<K, V>) commands).lrem(key, -1, members[0]);
	}

	public static <T> RpushMemberBuilder<T> key(String key) {
		return key(t -> key);
	}

	public static <T> RpushMemberBuilder<T> key(Converter<T, String> key) {
		return new RpushMemberBuilder<>(key);
	}

	public static class RpushMemberBuilder<T> {

		private final Converter<T, String> key;

		public RpushMemberBuilder(Converter<T, String> key) {
			this.key = key;
		}

		@SuppressWarnings("unchecked")
		public RpushBuilder<T> members(Converter<T, String>... members) {
			return new RpushBuilder<>(key, new ArrayConverter<>(String.class, members));
		}

		public RpushBuilder<T> members(Converter<T, String[]> members) {
			return new RpushBuilder<>(key, members);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class RpushBuilder<T> extends RemoveBuilder<T, RpushBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, String[]> members;

		public RpushBuilder(Converter<T, String> key, Converter<T, String[]> members) {
			super(members);
			this.key = key;
			this.members = members;
		}

		@Override
		public Rpush<String, String, T> build() {
			return new Rpush<>(key, del, remove, members);
		}

	}
}
