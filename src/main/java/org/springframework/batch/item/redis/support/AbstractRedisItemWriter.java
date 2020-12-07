package org.springframework.batch.item.redis.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisItemWriter<T> extends AbstractItemStreamItemWriter<T> {

	@Getter
	private final AbstractRedisClient client;
	private final GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig;
	private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;
	private final long commandTimeout;
	private GenericObjectPool<? extends StatefulConnection<String, String>> pool;

	protected AbstractRedisItemWriter(AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) {
		setName(ClassUtils.getShortName(getClass()));
		Assert.notNull(client, "A Redis client is required.");
		Assert.notNull(poolConfig, "A connection pool config is required.");
		this.client = client;
		this.poolConfig = poolConfig;
		this.async = ClientUtils.async(client);
		this.commandTimeout = client.getDefaultTimeout().getSeconds();
	}

	@Override
	public void open(ExecutionContext executionContext) {
		pool = ClientUtils.connectionPool(client, poolConfig);
		super.open(executionContext);
	}

	@Override
	public void close() {
		if (pool != null) {
			log.info("Closing connection pool");
			pool.close();
		}
		super.close();
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<String, String> connection = pool.borrowObject()) {
			BaseRedisAsyncCommands<String, String> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = write(commands, items);
			commands.flushCommands();
			for (RedisFuture<?> future : futures) {
				try {
					future.get(commandTimeout, TimeUnit.SECONDS);
				} catch (ExecutionException e) {
					log.error("Could not write item", e.getCause());
				} catch (TimeoutException e) {
					log.error("Command timed out", e);
				}
			}
			commands.setAutoFlushCommands(true);
		}
	}

	protected List<RedisFuture<?>> write(BaseRedisAsyncCommands<String, String> commands, List<? extends T> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (T item : items) {
			futures.add(write(commands, item));
		}
		return futures;
	}

	protected abstract RedisFuture<?> write(BaseRedisAsyncCommands<String, String> commands, T item);

}
