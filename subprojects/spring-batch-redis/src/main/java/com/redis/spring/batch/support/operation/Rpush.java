package com.redis.spring.batch.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.support.convert.ArrayConverter;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Rpush<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, V[]> members;

	public Rpush(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V[]> members) {
		super(key, delete, remove);
		this.members = members;
	}

	@Override
	protected RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		return commands.rpush(key, members.convert(item));
	}

	@Override
	protected RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		V[] members = this.members.convert(item);
		if (members == null) {
			return null;
		}
		if (members.length > 1) {
			throw new RedisCommandExecutionException("Removal of multiple list members is not supported");
		}
		return commands.lrem(key, -1, members[0]);
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
