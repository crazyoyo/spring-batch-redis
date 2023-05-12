package com.redis.spring.batch.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.ClassUtils;
import org.springframework.util.FileCopyUtils;

import com.redis.spring.batch.common.ConnectionPoolBuilder;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractReadOperation<K, V, T> extends ItemStreamSupport implements ReadOperation<K, T> {

	private static final Log log = LogFactory.getLog(AbstractReadOperation.class);

	private static final String ABSTTL_LUA = "absttl.lua";

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private final PoolOptions poolOptions;
	private GenericObjectPool<StatefulConnection<K, V>> pool;
	private String digest;

	protected AbstractReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec, PoolOptions poolOptions) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.codec = codec;
		this.poolOptions = poolOptions;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (pool == null) {
			byte[] bytes;
			try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(ABSTTL_LUA)) {
				bytes = FileCopyUtils.copyToByteArray(inputStream);
			} catch (IOException e) {
				throw new ItemStreamException("Could not load LUA script file " + ABSTTL_LUA);
			}
			pool = ConnectionPoolBuilder.client(client).options(poolOptions).codec(codec);
			try (StatefulConnection<K, V> connection = pool.borrowObject()) {
				RedisScriptingAsyncCommands<K, V> commands = Utils.async(connection);
				RedisFuture<String> future = commands.scriptLoad(bytes);
				this.digest = future.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				log.warn("Interrupted!", e);
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				throw new ItemStreamException("Could not open reader", e);
			}
		}
	}

	@Override
	public synchronized void close() {
		if (pool != null) {
			log.info("Closing pool");
			pool.close();
			pool = null;
		}
		super.close();
	}

	protected <U> U get(RedisFuture<U> future, StatefulConnection<K, V> connection)
			throws InterruptedException, ExecutionException, TimeoutException {
		return future.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
	}

	@SuppressWarnings("unchecked")
	protected RedisFuture<Long> absoluteTTL(BaseRedisAsyncCommands<K, V> commands, K key) {
		return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest, ScriptOutputType.INTEGER, key);
	}

	@Override
	public List<T> read(List<? extends K> keys) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			try {
				return read(connection, keys);
			} finally {
				connection.setAutoFlushCommands(true);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw e;
		}
	}

	protected abstract List<T> read(StatefulConnection<K, V> connection, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException;

}
