package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamSupport;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class OperationItemProcessor<K, V, I, O> extends ItemStreamSupport
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

	public PoolOptions getPoolOptions() {
		return poolOptions;
	}

	public void setPoolOptions(PoolOptions poolOptions) {
		this.poolOptions = poolOptions;
	}

	public Optional<ReadFrom> getReadFrom() {
		return readFrom;
	}

	public void setReadFrom(Optional<ReadFrom> readFrom) {
		this.readFrom = readFrom;
	}

	public OperationItemProcessor(AbstractRedisClient client, RedisCodec<K, V> codec,
			BatchOperation<K, V, I, O> operation) {
		this.client = client;
		this.codec = codec;
		this.operation = operation;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		if (!isOpen()) {
			ConnectionPoolFactory poolFactory = ConnectionPoolFactory.client(client);
			poolFactory.withOptions(poolOptions);
			poolFactory.withReadFrom(readFrom);
			pool = poolFactory.build(codec);
		}
		super.open(executionContext);
	}

	public boolean isOpen() {
		return pool != null;
	}

	@Override
	public synchronized void close() {
		super.close();
		if (isOpen()) {
			pool.close();
			pool = null;
		}
	}

	@Override
	public List<O> process(List<? extends I> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			long timeout = connection.getTimeout().toMillis();
			connection.setAutoFlushCommands(false);
			try {
				BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
				List<Future<O>> futures = operation.execute(commands, items);
				connection.flushCommands();
				List<O> results = new ArrayList<>(futures.size());
				for (Future<O> future : futures) {
					results.add(future.get(timeout, TimeUnit.MILLISECONDS));
				}
				return results;
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

}