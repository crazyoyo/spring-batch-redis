package com.redis.spring.batch.support;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.FileCopyUtils;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public abstract class AbstractValueReader<K, V, T extends KeyValue<K, ?>> extends ConnectionPoolItemStream<K, V>
		implements ItemProcessor<List<? extends K>, List<T>> {

	private final Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async;
	private String digest;

	protected AbstractValueReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async) {
		super(connectionSupplier, poolConfig);
		this.async = async;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (digest == null) {
			try (StatefulConnection<K, V> connection = pool.borrowObject()) {
				long timeout = connection.getTimeout().toMillis();
				RedisScriptingAsyncCommands<K, V> commands = async.apply(connection);
				byte[] bytes = FileCopyUtils
						.copyToByteArray(getClass().getClassLoader().getResourceAsStream("absttl.lua"));
				RedisFuture<String> load = commands.scriptLoad(bytes);
				this.digest = load.get(timeout, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				throw new ItemStreamException("Could not open reader", e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected RedisFuture<Long> absoluteTTL(RedisModulesAsyncCommands<K, V> commands, K... keys) {
		return commands.evalsha(digest, ScriptOutputType.INTEGER, keys);
	}

	@Override
	public List<T> process(List<? extends K> keys) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			RedisModulesAsyncCommands<K, V> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			try {
				return read(commands, connection.getTimeout().toMillis(), keys);
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	protected abstract List<T> read(RedisModulesAsyncCommands<K, V> commands, long timeout, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException;

	public static interface ValueReaderFactory<K, V, T extends KeyValue<K, ?>, R extends ItemProcessor<List<? extends K>, List<T>>> {

		R create(Supplier<StatefulConnection<K, V>> connectionSupplier,
				GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
				Function<StatefulConnection<K, V>, RedisModulesAsyncCommands<K, V>> async);

	}

}
