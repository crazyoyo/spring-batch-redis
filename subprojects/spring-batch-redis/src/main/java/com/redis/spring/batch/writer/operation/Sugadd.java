package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;

import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.Suggestion;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractOperation<K, V, T> {

	protected final Function<T, Suggestion<V>> suggestion;

	public Sugadd(Function<T, K> key, Function<T, Suggestion<V>> suggestion) {
		super(key);
		Assert.notNull(suggestion, "A suggestion function is required");
		this.suggestion = suggestion;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item, K key) {
		futures.add(execute((RediSearchAsyncCommands<K, V>) commands, key, suggestion.apply(item)));
	}

	protected RedisFuture<Long> execute(RediSearchAsyncCommands<K, V> commands, K key, Suggestion<V> suggestion) {
		return commands.ftSugadd(key, suggestion);
	}

}
