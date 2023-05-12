package com.redis.spring.batch.writer;

import java.util.List;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.writer.operation.Del;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class OverwriteOperation<K, V> implements WriteOperation<K, V, DataStructure<K>> {

	private final Del<K, V, DataStructure<K>> delete = new Del<>(DataStructure::getKey);
	private final WriteOperation<K, V, DataStructure<K>> operation;

	public OverwriteOperation(WriteOperation<K, V, DataStructure<K>> operation) {
		this.operation = operation;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, DataStructure<K> item) {
		delete.execute(commands, futures, item);
		operation.execute(commands, futures, item);
	}

}
