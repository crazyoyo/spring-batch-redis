package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemStreamSupport<K, V, I, O> extends DelegatingItemStreamSupport
		implements ItemProcessor<List<? extends I>, List<O>> {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final PoolOptions poolOptions;
	private final BatchOperation<K, V, I, O> operation;

	private GenericObjectPool<StatefulConnection<K, V>> pool;

	public OperationItemStreamSupport(AbstractRedisClient client, RedisCodec<K, V> codec, PoolOptions poolOptions,
			BatchOperation<K, V, I, O> operation) {
		super(operation);
		this.client = client;
		this.codec = codec;
		this.poolOptions = poolOptions;
		this.operation = operation;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (pool == null) {
			this.pool = ConnectionPoolBuilder.client(client).options(poolOptions).codec(codec);
		}
	}

	@Override
	public synchronized void close() {
		if (pool != null) {
			pool.close();
			pool = null;
		}
		super.close();
	}

	@Override
	public List<O> process(List<? extends I> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			long timeout = connection.getTimeout().toMillis();
			connection.setAutoFlushCommands(false);
			try {
				BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
				List<RedisFuture<O>> futures = operation.execute(commands, items);
				connection.flushCommands();
				List<O> results = new ArrayList<>(futures.size());
				for (RedisFuture<O> future : futures) {
					O result;
					try {
						result = future.get(timeout, TimeUnit.MILLISECONDS);
					} catch (ExecutionException e) {
						throw e;
					}
					results.add(result);
				}
				return results;
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

}