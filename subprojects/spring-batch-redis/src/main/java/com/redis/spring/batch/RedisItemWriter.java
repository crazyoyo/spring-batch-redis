package com.redis.spring.batch;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.writer.DataStructureOperation;
import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.PipelinedOperation;
import com.redis.spring.batch.writer.SimplePipelinedOperation;
import com.redis.spring.batch.writer.WriterBuilder;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;

public class RedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

	private final GenericObjectPool<StatefulConnection<K, V>> pool;
	private final PipelinedOperation<K, V, T> operation;
	private boolean open;

	public RedisItemWriter(GenericObjectPool<StatefulConnection<K, V>> pool, PipelinedOperation<K, V, T> operation) {
		this.pool = pool;
		this.operation = operation;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		this.open = true;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			try {
				Collection<RedisFuture> futures = operation.execute(connection, items);
				connection.flushCommands();
				long timeout = connection.getTimeout().toMillis();
				LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new Future[0]));
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

	public boolean isOpen() {
		return open;
	}

	@Override
	public void close() {
		super.close();
		this.open = false;
	}

	public static <K, V, T> WriterBuilder<K, V, T> operation(GenericObjectPool<StatefulConnection<K, V>> connectionPool,
			Operation<K, V, T> operation) {
		return new WriterBuilder<>(connectionPool, operation);
	}

	public static <K, V> WriterBuilder<K, V, DataStructure<K>> dataStructure(
			GenericObjectPool<StatefulConnection<K, V>> pool) {
		return new WriterBuilder<>(pool, new DataStructureOperation<>());
	}

	public static <K, V> WriterBuilder<K, V, DataStructure<K>> dataStructure(
			GenericObjectPool<StatefulConnection<K, V>> pool, Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
		return new WriterBuilder<>(pool, new DataStructureOperation<>(xaddArgs));
	}

	public static <K, V> WriterBuilder<K, V, KeyDump<K>> keyDump(GenericObjectPool<StatefulConnection<K, V>> pool) {
		return new WriterBuilder<>(pool, SimplePipelinedOperation.keyDump());
	}
}
