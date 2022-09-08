package com.redis.spring.batch.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.FileCopyUtils;

import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public abstract class AbstractValueReader<K, V, T> extends ItemStreamSupport
		implements ItemProcessor<List<? extends K>, List<T>> {

	private final Log log = LogFactory.getLog(getClass());

	private static final String ABSTTL_LUA = "absttl.lua";

	private final GenericObjectPool<StatefulConnection<K, V>> connectionPool;
	private String digest;

	protected AbstractValueReader(GenericObjectPool<StatefulConnection<K, V>> connectionPool) {
		Assert.notNull(connectionPool, "Right connection pool is required");
		setName(ClassUtils.getShortName(getClass()));
		this.connectionPool = connectionPool;
	}

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
			try (StatefulConnection<K, V> connection = connectionPool.borrowObject()) {
				RedisScriptingAsyncCommands<K, V> commands = Utils.async(connection);
				RedisFuture<String> future = commands.scriptLoad(bytes);
				this.digest = future.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
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
	public List<T> process(List<? extends K> item) throws Exception {
		try (StatefulConnection<K, V> connection = connectionPool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			try {
				return read(connection, item);
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

	protected abstract List<T> read(StatefulConnection<K, V> connection, List<? extends K> keys) throws Exception;

}
