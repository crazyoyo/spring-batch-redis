package org.springframework.batch.item.redis.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import io.lettuce.core.RedisFuture;
import org.springframework.core.convert.converter.Converter;

import java.util.function.Predicate;

public class Sadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	private final Converter<T, V[]> members;

	public Sadd(Converter<T, K> key, Predicate<T> delete, Predicate<T> remove, Converter<T, V[]> members) {
		super(key, delete, remove);
		this.members = members;
	}

	@Override
	protected RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		return commands.sadd(key, members.convert(item));
	}

	@Override
	protected RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		return commands.srem(key, members.convert(item));
	}

	public static <T> SaddMemberBuilder<String, T> key(String key) {
		return key(t -> key);
	}

	public static <K, T> SaddMemberBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <K, T> SaddMemberBuilder<K, T> key(Converter<T, K> key) {
		return new SaddMemberBuilder<>(key);
	}

	public static class SaddMemberBuilder<K, T> {

		private final Converter<T, K> key;

		public SaddMemberBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> SaddBuilder<K, V, T> members(Converter<T, V[]> members) {
			return new SaddBuilder<>(key, members);
		}
	}

	public static class SaddBuilder<K, V, T> extends RemoveBuilder<K, V, T, Rpush.RpushBuilder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, V[]> members;

		public SaddBuilder(Converter<T, K> key, Converter<T, V[]> members) {
			super(members);
			this.key = key;
			this.members = members;
		}

		@Override
		public Sadd<K, V, T> build() {
			return new Sadd<>(key, del, remove, members);
		}

	}

}
