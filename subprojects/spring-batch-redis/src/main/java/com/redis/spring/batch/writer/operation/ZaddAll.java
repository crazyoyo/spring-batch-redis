package com.redis.spring.batch.writer.operation;

import java.util.Collection;
import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;

public class ZaddAll<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Converter<T, Collection<ScoredValue<V>>> members;

	public ZaddAll(Converter<T, K> key, Predicate<T> delete, Converter<T, Collection<ScoredValue<V>>> members) {
		super(key, delete);
		this.members = members;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Collection<ScoredValue<V>> zset = members.convert(item);
		if (zset == null) {
			return null;
		}
		return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, zset.toArray(new ScoredValue[0]));
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

		public <V> Builder<K, V, T> members(Converter<T, Collection<ScoredValue<V>>> members) {
			return new Builder<>(key, members);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Collection<ScoredValue<V>>> members;

		public Builder(Converter<T, K> key, Converter<T, Collection<ScoredValue<V>>> members) {
			this.key = key;
			this.members = members;
			onNull(members);
		}

		public ZaddAll<K, V, T> build() {
			return new ZaddAll<>(key, del, members);
		}

	}

}
