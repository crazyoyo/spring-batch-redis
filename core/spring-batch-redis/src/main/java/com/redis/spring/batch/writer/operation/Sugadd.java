package com.redis.spring.batch.writer.operation;

import java.util.function.Function;
import java.util.function.Predicate;

import org.springframework.batch.item.Chunk;

import com.redis.lettucemod.api.async.RediSearchAsyncCommands;
import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractKeyWriteOperation<K, V, T> {

	private final Function<T, Suggestion<V>> suggestionFunction;

	private Predicate<T> incrPredicate = Predicates.isFalse();

	public Sugadd(Function<T, K> keyFunction, Function<T, Suggestion<V>> suggestionFunction) {
		super(keyFunction);
		this.suggestionFunction = suggestionFunction;
	}

	public void setIncr(boolean incr) {
		this.incrPredicate = t -> incr;
	}

	public void setIncrPredicate(Predicate<T> predicate) {
		this.incrPredicate = predicate;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Chunk<RedisFuture<Object>> outputs) {
		outputs.add((RedisFuture) execute((RediSearchAsyncCommands<K, V>) commands, item, key));
	}

	private RedisFuture<Long> execute(RediSearchAsyncCommands<K, V> commands, T item, K key) {
		Suggestion<V> suggestion = suggestionFunction.apply(item);
		if (incrPredicate.test(item)) {
			return commands.ftSugaddIncr(key, suggestion);
		}
		return commands.ftSugadd(key, suggestion);
	}

}
