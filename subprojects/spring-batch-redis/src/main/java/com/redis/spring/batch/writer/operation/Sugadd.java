package com.redis.spring.batch.writer.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.Suggestion;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractKeyOperation<K, V, T> {

	private final Predicate<T> remove;
	protected final Converter<T, Suggestion<V>> suggestionConverter;
	private final boolean incr;

	public Sugadd(Converter<T, K> key, Predicate<T> remove, Converter<T, Suggestion<V>> suggestion, boolean incr) {
		super(key, t -> false);
		Assert.notNull(remove, "A remove predicate is required");
		Assert.notNull(suggestion, "A suggestion converter is required");
		this.remove = remove;
		this.suggestionConverter = suggestion;
		this.incr = incr;
	}

	@Override
	protected RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		if (remove.test(item)) {
			return remove(commands, item, key);
		}
		return add(commands, item, key);
	}

	@SuppressWarnings("unchecked")
	protected RedisFuture<Long> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Suggestion<V> suggestion = this.suggestionConverter.convert(item);
		if (incr) {
			return ((RediSearchAsyncCommands<K, V>) commands).ftSugaddIncr(key, suggestion);
		}
		return ((RediSearchAsyncCommands<K, V>) commands).ftSugadd(key, suggestion);
	}

	@SuppressWarnings("unchecked")
	protected RedisFuture<Boolean> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Suggestion<V> suggestion = this.suggestionConverter.convert(item);
		if (suggestion == null) {
			return null;
		}
		return ((RediSearchAsyncCommands<K, V>) commands).ftSugdel(key, suggestion.getString());
	}

	public static <K, T> SuggestionBuilder<K, T> key(K key) {
		return key(t -> key);
	}

	public static <K, T> SuggestionBuilder<K, T> key(Converter<T, K> key) {
		return new SuggestionBuilder<>(key);
	}

	public static class SuggestionBuilder<K, T> {

		private final Converter<T, K> key;

		public SuggestionBuilder(Converter<T, K> key) {
			this.key = key;
		}

		public <V> Builder<K, V, T> suggestion(Converter<T, Suggestion<V>> suggestion) {
			return new Builder<>(key, suggestion);
		}
	}

	public static class Builder<K, V, T> extends DelBuilder<K, V, T, Builder<K, V, T>> {

		private final Converter<T, K> key;
		private final Converter<T, Suggestion<V>> suggestion;
		private boolean increment;

		public Builder(Converter<T, K> key, Converter<T, Suggestion<V>> suggestion) {
			this.key = key;
			this.suggestion = suggestion;
			onNull(suggestion);
		}

		public Builder<K, V, T> increment(boolean increment) {
			this.increment = increment;
			return this;
		}

		public Sugadd<K, V, T> build() {
			return new Sugadd<>(key, del, suggestion, increment);
		}
	}

}
