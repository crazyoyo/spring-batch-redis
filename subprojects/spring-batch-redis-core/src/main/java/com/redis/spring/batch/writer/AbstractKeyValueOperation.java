package com.redis.spring.batch.writer;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public abstract class AbstractKeyValueOperation<K, V, T, I> extends AbstractKeyOperation<K, V, I> {

	private final Function<I, T> valueFunction;
	private final Predicate<T> skipPredicate;

	protected AbstractKeyValueOperation(Function<I, K> keyFunction, Function<I, T> valueFunction) {
		this(keyFunction, valueFunction, Objects::isNull);
	}

	protected AbstractKeyValueOperation(Function<I, K> keyFunction, Function<I, T> valueFunction,
			Predicate<T> skipPredicate) {
		super(keyFunction);
		this.skipPredicate = skipPredicate;
		this.valueFunction = valueFunction;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void execute(BaseRedisAsyncCommands<K, V> commands, I item, K key, List<RedisFuture<Object>> outputs) {
		T value = valueFunction.apply(item);
		if (skipPredicate.test(value)) {
			return;
		}
		outputs.add(execute(commands, item, key, value));
	}

	@SuppressWarnings("rawtypes")
	protected abstract RedisFuture execute(BaseRedisAsyncCommands<K, V> commands, I item, K key, T value);

}
