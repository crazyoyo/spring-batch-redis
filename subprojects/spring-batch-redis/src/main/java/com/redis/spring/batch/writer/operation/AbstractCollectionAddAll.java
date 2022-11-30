package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

import com.redis.spring.batch.writer.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractCollectionAddAll<K, V, T> {

	private final Predicate<T> delPredicate;
	private final Operation<K, V, T> del;

	protected AbstractCollectionAddAll(Predicate<T> delPredicate, Operation<K, V, T> del) {
		this.delPredicate = delPredicate;
		this.del = del;
	}

	@SuppressWarnings("rawtypes")
	public Collection<RedisFuture> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
		Collection<RedisFuture> futures = new ArrayList<>();
		if (delPredicate.test(item)) {
			futures.add(del.execute(commands, item));
		}
		futures.addAll(doExecute(commands, item));
		return futures;
	}

	@SuppressWarnings("rawtypes")
	protected abstract Collection<RedisFuture> doExecute(BaseRedisAsyncCommands<K, V> commands, T item);

}
