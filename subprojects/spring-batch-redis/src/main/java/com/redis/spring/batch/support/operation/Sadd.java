package com.redis.spring.batch.support.operation;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.support.convert.ArrayConverter;

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

	public static <T> SaddMemberBuilder<T> key(String key) {
		return key(t -> key);
	}

	public static <T> SaddMemberBuilder<T> key(Converter<T, String> key) {
		return new SaddMemberBuilder<>(key);
	}

	public static class SaddMemberBuilder<T> {

		private final Converter<T, String> key;

		public SaddMemberBuilder(Converter<T, String> key) {
			this.key = key;
		}
		
		@SuppressWarnings("unchecked")
		public SaddBuilder<T> members(Converter<T, String>... members) {
			return new SaddBuilder<>(key, new ArrayConverter<>(String.class, members));
		}

		public SaddBuilder<T> members(Converter<T, String[]> members) {
			return new SaddBuilder<>(key, members);
		}
	}

	public static class SaddBuilder<T> extends RemoveBuilder<T, SaddBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, String[]> members;

		public SaddBuilder(Converter<T, String> key, Converter<T, String[]> members) {
			super(members);
			this.key = key;
			this.members = members;
		}

		@Override
		public Sadd<String, String, T> build() {
			return new Sadd<>(key, del, remove, members);
		}

	}

}
