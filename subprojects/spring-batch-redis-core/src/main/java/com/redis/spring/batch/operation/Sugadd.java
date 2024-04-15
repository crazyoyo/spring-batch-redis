package com.redis.spring.batch.operation;

import java.util.function.Function;
import java.util.function.Predicate;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractKeyValueOperation<K, V, Suggestion<V>, T> {

	private Predicate<T> incrPredicate = Predicates.isFalse();

	public Sugadd(Function<T, K> keyFunction, Function<T, Suggestion<V>> suggestionFunction) {
		super(keyFunction, suggestionFunction);
	}

	public void setIncr(boolean incr) {
		this.incrPredicate = t -> incr;
	}

	public void setIncrPredicate(Predicate<T> predicate) {
		this.incrPredicate = predicate;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, Suggestion<V> value) {
		if (incrPredicate.test(item)) {
			return ((RedisModulesAsyncCommands<K, V>) commands).ftSugaddIncr(key, value);
		}
		return ((RedisModulesAsyncCommands<K, V>) commands).ftSugadd(key, value);
	}

}
