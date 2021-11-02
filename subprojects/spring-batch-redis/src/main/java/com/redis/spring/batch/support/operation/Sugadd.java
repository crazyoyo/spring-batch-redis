package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.api.search.Suggestion;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Sugadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	protected final Converter<T, Suggestion<V>> suggestionConverter;
	private final boolean incr;

	public Sugadd(Converter<T, K> key, Predicate<T> remove, Converter<T, Suggestion<V>> suggestion, boolean incr) {
		super(key, t -> false, remove);
		Assert.notNull(suggestion, "A suggestion converter is required");
		this.suggestionConverter = suggestion;
		this.incr = incr;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> add(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Suggestion<V> suggestion = this.suggestionConverter.convert(item);
		if (incr) {
			return ((RediSearchAsyncCommands<K, V>) commands).sugaddIncr(key, suggestion);
		}
		return ((RediSearchAsyncCommands<K, V>) commands).sugadd(key, suggestion);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Boolean> remove(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		Suggestion<V> suggestion = this.suggestionConverter.convert(item);
		if (suggestion == null) {
			return null;
		}
		return ((RediSearchAsyncCommands<K, V>) commands).sugdel(key, suggestion.getString());
	}

	public static <T> SugaddSuggestionBuilder<T> key(String key) {
		return new SugaddSuggestionBuilder<>(t -> key);
	}

	public static <T> SugaddSuggestionBuilder<T> key(Converter<T, String> key) {
		return new SugaddSuggestionBuilder<>(key);
	}

	public static class SugaddSuggestionBuilder<T> {

		private final Converter<T, String> key;

		public SugaddSuggestionBuilder(Converter<T, String> key) {
			this.key = key;
		}

		public SugaddBuilder<T> suggestion(Converter<T, Suggestion<String>> suggestion) {
			return new SugaddBuilder<>(key, suggestion);
		}
	}

	@Setter
	@Accessors(fluent = true)
	public static class SugaddBuilder<T> extends DelBuilder<T, SugaddBuilder<T>> {

		private final Converter<T, String> key;
		private final Converter<T, Suggestion<String>> suggestion;
		private boolean increment;

		public SugaddBuilder(Converter<T, String> key, Converter<T, Suggestion<String>> suggestion) {
			super(suggestion);
			this.key = key;
			this.suggestion = suggestion;
		}

		@Override
		public Sugadd<String, String, T> build() {
			return new Sugadd<>(key, del, suggestion, increment);
		}
	}

}
