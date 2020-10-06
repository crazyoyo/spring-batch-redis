package org.springframework.batch.item.redis.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.ClassUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRedisItemWriter<K, V, T> extends AbstractItemStreamItemWriter<T> {

	@Setter
	private GenericObjectPool<? extends StatefulConnection<K, V>> pool;
	@Setter
	private Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands;
	private long commandTimeout;

	protected AbstractRedisItemWriter() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public void setCommandTimeout(Duration commandTimeout) {
		this.commandTimeout = commandTimeout.getSeconds();
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			BaseRedisAsyncCommands<K, V> commands = this.commands.apply(connection);
			commands.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = new ArrayList<>();
			for (T item : items) {
				write(commands, futures, item);
			}
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

	protected abstract void write(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item);

}
