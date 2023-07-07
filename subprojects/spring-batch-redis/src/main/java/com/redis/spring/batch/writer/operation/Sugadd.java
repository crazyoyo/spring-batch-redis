package com.redis.spring.batch.writer.operation;

import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.Suggestion;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractWriteOperation<K, V, T> {

	protected final Function<T, Suggestion<V>> suggestion;

	public Sugadd(Function<T, K> key, Function<T, Suggestion<V>> suggestion) {
		super(key);
		Assert.notNull(suggestion, "A suggestion function is required");
		this.suggestion = suggestion;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<Long> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		return execute((RediSearchAsyncCommands<K, V>) commands, key, suggestion.apply(item));
	}

	protected RedisFuture<Long> execute(RediSearchAsyncCommands<K, V> commands, K key, Suggestion<V> suggestion) {
		return commands.ftSugadd(key, suggestion);
	}

}
