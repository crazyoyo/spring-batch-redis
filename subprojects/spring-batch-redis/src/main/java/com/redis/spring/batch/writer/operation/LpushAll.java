package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;

public class LpushAll<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, Collection<V>> members;

	public LpushAll(Converter<T, K> key, Predicate<T> delete, Converter<T, Collection<V>> members) {
		super(key, delete);
		this.members = members;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Collection<V> collection = members.convert(item);
		if (collection == null || collection.isEmpty()) {
			return null;
		}
		return ((RedisListAsyncCommands<K, V>) commands).lpush(key, (V[]) collection.toArray());
	}

	public static <K, T> MembersBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <K, T> MembersBuilder<K, T> key(Converter<T, K> key) {
		return new MembersBuilder<>(key);
	}

	public static class MembersBuilder<K, T> {

		private final Converter<T, K> key;

		public MembersBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> members(Converter<T, Collection<V>> members) {
			return new Builder<>(key, members);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Collection<V>> members;

		public Builder(Converter<T, K> key, Converter<T, Collection<V>> members) {
			this.key = key;
			this.members = members;
			onNull(members);
		}

		public LpushAll<K, V, T> build() {
			return new LpushAll<>(key, del, members);
		}

	}
}
