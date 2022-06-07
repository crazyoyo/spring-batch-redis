package com.redis.spring.batch.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.FileCopyUtils;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.support.ConnectionPoolItemStream;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public abstract class AbstractValueReader<K, V, T extends KeyValue<K, ?>> extends ConnectionPoolItemStream<K, V>
		implements ValueReader<K, T> {

	private static final Logger log = LoggerFactory.getLogger(AbstractValueReader.class);

	private static final String ABSTTL_LUA = "absttl.lua";
	private final Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async;
	private String digest;

	protected AbstractValueReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async) {
		super(connectionSupplier, poolConfig);
		this.async = async;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (digest == null) {
			byte[] bytes;
			try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(ABSTTL_LUA)) {
				bytes = FileCopyUtils.copyToByteArray(inputStream);
			} catch (IOException e) {
				throw new ItemStreamException("Could not load LUA script file " + ABSTTL_LUA);
			}
			try (StatefulConnection<K, V> connection = borrowConnection()) {
				long timeout = connection.getTimeout().toMillis();
				RedisFuture<String> load = ((RedisScriptingAsyncCommands<K, V>) async.apply(connection))
						.scriptLoad(bytes);
				this.digest = load.get(timeout, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				log.warn("Interrupted!", e);
				// Restore interrupted state...
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				throw new ItemStreamException("Could not open reader", e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected RedisFuture<Long> absoluteTTL(BaseRedisAsyncCommands<K, V> commands, K... keys) {
		return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest, ScriptOutputType.INTEGER, keys);
	}

	@Override
	public List<T> read(List<? extends K> keys) throws Exception {
		try (StatefulConnection<K, V> connection = borrowConnection()) {
			BaseRedisAsyncCommands<K, V> commands = async.apply(connection);
			commands.setAutoFlushCommands(false);
			try {
				return read(commands, connection.getTimeout().toMillis(), keys);
			} finally {
				commands.setAutoFlushCommands(true);
			}
		}
	}

	protected abstract List<T> read(BaseRedisAsyncCommands<K, V> commands, long timeout, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException;

	public static interface ValueReaderFactory<K, V, T extends KeyValue<K, ?>> {

		ValueReader<K, T> create(Supplier<StatefulConnection<K, V>> connectionSupplier,
				GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
				Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async);

	}

}
