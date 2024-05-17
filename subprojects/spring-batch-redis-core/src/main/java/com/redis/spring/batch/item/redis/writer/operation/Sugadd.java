package com.redis.spring.batch.item.redis.writer.operation;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.search.Suggestion;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.writer.AbstractValueWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class Sugadd<K, V, T> extends AbstractValueWriteOperation<K, V, Suggestion<V>, T> {

	private Predicate<T> incrPredicate = t -> false;

	public Sugadd(Function<T, K> keyFunction, Function<T, Suggestion<V>> valueFunction) {
		super(keyFunction, valueFunction);
	}

	public void setIncr(boolean incr) {
		this.incrPredicate = t -> incr;
	}

	public void setIncrPredicate(Predicate<T> predicate) {
		this.incrPredicate = predicate;
	}

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends T> items) {
		return BatchUtils.executeAll(commands, items, this::execute);
	}

	private RedisFuture<?> execute(RedisAsyncCommands<K, V> commands, T item) {
		if (incrPredicate.test(item)) {
			return ((RedisModulesAsyncCommands<K, V>) commands).ftSugaddIncr(key(item), value(item));
		}
		return ((RedisModulesAsyncCommands<K, V>) commands).ftSugadd(key(item), value(item));
	}

}
