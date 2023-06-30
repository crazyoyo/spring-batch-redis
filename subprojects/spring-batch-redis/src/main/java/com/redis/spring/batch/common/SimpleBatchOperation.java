package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class SimpleBatchOperation<K, V, I, O> extends DelegatingItemStreamSupport
		implements BatchOperation<K, V, I, O> {

	private final Operation<K, V, I, O> operation;

	public SimpleBatchOperation(Operation<K, V, I, O> operation) {
		super(operation);
		this.operation = operation;
	}

	@Override
	public List<Future<O>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items) {
		List<Future<O>> futures = new ArrayList<>();
		for (I item : items) {
			futures.add(operation.execute(commands, item));
		}
		return futures;
	}

	public static <K, V, I, O> BatchOperation<K, V, I, O> of(Operation<K, V, I, O> operation) {
		return new SimpleBatchOperation<>(operation);
	}

}
