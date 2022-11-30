package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.writer.operation.RestoreReplace;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;

public class SimplePipelinedOperation<K, V, T> implements PipelinedOperation<K, V, T> {

	private final Operation<K, V, T> operation;

	public SimplePipelinedOperation(Operation<K, V, T> operation) {
		this.operation = operation;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Collection<RedisFuture> execute(StatefulConnection<K, V> connection, List<? extends T> items) {
		Collection<RedisFuture> futures = new ArrayList<>();
		for (T item : items) {
			RedisFuture<?> future = operation.execute(Utils.async(connection), item);
			if (future == null) {
				continue;
			}
			futures.add(future);
		}
		return futures;
	}

	public static <K, V, T> SimplePipelinedOperation<K, V, T> of(Operation<K, V, T> operation) {
		return new SimplePipelinedOperation<>(operation);
	}

	public static <K, V> SimplePipelinedOperation<K, V, KeyDump<K>> keyDump() {
		return new SimplePipelinedOperation<>(RestoreReplace.keyDump());
	}

}
