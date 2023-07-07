package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemProcessor<K, V, I, O> extends DelegatingItemStreamSupport
		implements ItemProcessor<List<? extends I>, List<O>> {

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final BatchOperation<K, V, I, O> operation;

	private PoolOptions poolOptions = PoolOptions.builder().build();
	private Optional<ReadFrom> readFrom = Optional.empty();
	private GenericObjectPool<StatefulConnection<K, V>> pool;

	public OperationItemProcessor(AbstractRedisClient client, RedisCodec<K, V> codec, Operation<K, V, I, O> operation) {
		this(client, codec, new SimpleBatchOperation<>(operation));
	}

	public OperationItemProcessor(AbstractRedisClient client, RedisCodec<K, V> codec,
			BatchOperation<K, V, I, O> operation) {
		super(operation);
		this.client = client;
		this.codec = codec;
		this.operation = operation;
	}

	public void setReadFrom(Optional<ReadFrom> readFrom) {
		this.readFrom = readFrom;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (pool == null) {
			this.pool = ConnectionPoolFactory.client(client).withOptions(poolOptions).withReadFrom(readFrom)
					.codec(codec);
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
	public synchronized List<O> process(List<? extends I> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			long timeout = connection.getTimeout().toMillis();
			connection.setAutoFlushCommands(false);
			try {
				BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
				List<Future<O>> futures = operation.execute(commands, items);
				connection.flushCommands();
				List<O> results = new ArrayList<>(futures.size());
				for (Future<O> future : futures) {
					O result = future.get(timeout, TimeUnit.MILLISECONDS);
					results.add(result);
				}
				return results;
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

}