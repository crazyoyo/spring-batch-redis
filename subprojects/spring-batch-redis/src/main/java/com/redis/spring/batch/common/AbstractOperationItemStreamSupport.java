package com.redis.spring.batch.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.ClassUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractOperationItemStreamSupport<K, V, I, O> extends ItemStreamSupport {

	private final Object synchronizationLock = new Object();
	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private PoolOptions poolOptions = PoolOptions.builder().build();
	private Optional<ReadFrom> readFrom = Optional.empty();
	private GenericObjectPool<StatefulConnection<K, V>> pool;
	private Operation<K, V, I, O> operation;

	protected AbstractOperationItemStreamSupport(AbstractRedisClient client, RedisCodec<K, V> codec) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
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

	@Override
	public void open(ExecutionContext executionContext) {
		synchronized (synchronizationLock) {
			if (!isOpen()) {
				ConnectionPoolFactory poolFactory = ConnectionPoolFactory.client(client);
				poolFactory.withOptions(poolOptions);
				poolFactory.withReadFrom(readFrom);
				pool = poolFactory.build(codec);
				operation = operation();
			}
		}
		super.open(executionContext);
	}

	protected abstract Operation<K, V, I, O> operation();

	public boolean isOpen() {
		return pool != null;
	}

	@Override
	public void close() {
		super.close();
		synchronized (synchronizationLock) {
			if (isOpen()) {
				operation = null;
				pool.close();
				pool = null;
			}
		}
	}

	protected List<O> execute(List<? extends I> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			long timeout = connection.getTimeout().toMillis();
			BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
			List<RedisFuture<O>> futures = new ArrayList<>();
			try {
				connection.setAutoFlushCommands(false);
				execute(commands, items, futures);
				connection.flushCommands();
				List<O> results = new ArrayList<>(futures.size());
				for (RedisFuture<O> future : futures) {
					results.add(future.get(timeout, TimeUnit.MILLISECONDS));
				}
				return results;
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

	protected void execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items,
			List<RedisFuture<O>> futures) {
		for (I item : items) {
			operation.execute(commands, item, futures);
		}
	}

}