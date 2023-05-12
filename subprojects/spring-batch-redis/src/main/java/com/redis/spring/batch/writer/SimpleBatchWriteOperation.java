package com.redis.spring.batch.writer;

import java.util.List;

import org.springframework.util.Assert;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class SimpleBatchWriteOperation<K, V, T> implements BatchWriteOperation<K, V, T> {

	private final WriteOperation<K, V, T> operation;

	public SimpleBatchWriteOperation(WriteOperation<K, V, T> operation) {
		Assert.notNull(operation, "An operation is required");
		this.operation = operation;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, List<? extends T> items) {
		for (T item : items) {
			operation.execute(commands, futures, item);
		}
	}

}
