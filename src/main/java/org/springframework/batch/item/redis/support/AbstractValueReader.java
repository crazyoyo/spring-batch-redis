package org.springframework.batch.item.redis.support;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractValueReader<T> implements ValueReader<T> {

	private final AbstractRedisClient client;
	private final GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig;
	private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;
	private final long commandTimeout;
	private GenericObjectPool<? extends StatefulConnection<String, String>> pool;

	protected AbstractValueReader(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) {
		this.client = client;
		this.poolConfig = poolConfig;
		this.async = ClientUtils.async(client);
		this.commandTimeout = client.getDefaultTimeout().getSeconds();
	}

	@Override
	public void open(ExecutionContext executionContext) {
		pool = ClientUtils.connectionPool(client, poolConfig);
	}

	@Override
	public void close() {
		if (pool != null) {
			log.info("Closing connection pool");
			pool.close();
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
		// do nothing
	}

	@Override
	public List<T> read(List<? extends String> keys) throws Exception {
		try (StatefulConnection<String, String> connection = pool.borrowObject()) {
			BaseRedisAsyncCommands<String, String> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			try {
				return read(keys, commands);
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	protected abstract List<T> read(List<? extends String> keys, BaseRedisAsyncCommands<String, String> commands)
			throws Exception;

	protected <F> F get(RedisFuture<F> future) throws InterruptedException, ExecutionException, TimeoutException {
		if (future == null) {
			return null;
		}
		return future.get(commandTimeout, TimeUnit.SECONDS);
	}

	protected long getTtl(RedisFuture<Long> future) throws InterruptedException, ExecutionException, TimeoutException {
		Long ttl = get(future);
		if (ttl == null) {
			return 0;
		}
		return ttl;
	}

}
