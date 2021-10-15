package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

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

	public static <T> RpushMemberBuilder<String, T> key(String key) {
		return key(t -> key);
	}

	public static <K, T> RpushMemberBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <K, T> RpushMemberBuilder<K, T> key(Converter<T, K> key) {
		return new RpushMemberBuilder<>(key);
	}

	public static class RpushMemberBuilder<K, T> {

		private final Converter<T, K> key;

		public RpushMemberBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> RpushBuilder<K, V, T> members(Converter<T, V[]> members) {
			return new RpushBuilder<>(key, members);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class RpushBuilder<K, V, T> extends RemoveBuilder<K, V, T, RpushBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, V[]> members;

		public RpushBuilder(Converter<T, K> key, Converter<T, V[]> members) {
			super(members);
			this.key = key;
			this.members = members;
		}

		@Override
		public Rpush<K, V, T> build() {
			return new Rpush<>(key, del, remove, members);
		}

	}
}
