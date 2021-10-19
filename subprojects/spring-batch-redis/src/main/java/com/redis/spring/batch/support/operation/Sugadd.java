package com.redis.spring.batch.support.operation;

import java.util.function.Predicate;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.search.Suggestion;

import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Sugadd<K, V, T> extends AbstractCollectionOperation<K, V, T> {

	protected final Converter<T, Suggestion<V>> suggestion;
	private final boolean incr;

	public Sugadd(Converter<T, K> key, Predicate<T> remove, Converter<T, Suggestion<V>> suggestion, boolean incr) {
		super(key, t -> false, remove);
		Assert.notNull(suggestion, "A suggestion converter is required");
		this.suggestion = suggestion;
		this.incr = incr;
	}

	@Override
	protected RedisFuture<?> add(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		Suggestion<V> suggestion = this.suggestion.convert(item);
		if (incr) {
			return commands.sugaddIncr(key, suggestion);
		}
		return commands.sugadd(key, suggestion);
	}

	@Override
	protected RedisFuture<?> remove(RedisModulesAsyncCommands<K, V> commands, T item, K key) {
		Suggestion<V> suggestion = this.suggestion.convert(item);
		if (suggestion == null) {
			return null;
		}
		return commands.sugdel(key, suggestion.getString());
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
