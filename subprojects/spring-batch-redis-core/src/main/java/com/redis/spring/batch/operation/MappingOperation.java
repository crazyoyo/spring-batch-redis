package com.redis.spring.batch.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class MappingOperation<K, V, I, P, O> implements Operation<K, V, I, O> {

	private final Operation<K, V, I, P> delegate;
	private final Function<P, O> converter;

	public MappingOperation(Operation<K, V, I, P> delegate, Function<P, O> converter) {
		this.delegate = delegate;
		this.converter = converter;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends I> inputs,
			List<RedisFuture<O>> outputs) {
		List<RedisFuture<P>> pivots = new ArrayList<>();
		delegate.execute(commands, inputs, pivots);
		List<RedisFuture<O>> out = pivots.stream()
				.map(f -> new MappingRedisFuture<>(f.toCompletableFuture(), converter)).collect(Collectors.toList());
		outputs.addAll(out);
	}

}
