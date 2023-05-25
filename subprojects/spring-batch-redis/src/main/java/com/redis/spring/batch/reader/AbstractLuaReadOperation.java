package com.redis.spring.batch.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.util.ClassUtils;
import org.springframework.util.FileCopyUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.ConvertingRedisFuture;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public abstract class AbstractLuaReadOperation<K, V, T> extends ItemStreamSupport implements Operation<K, V, K, T> {

	private final AbstractRedisClient client;
	private final String filename;
	private String digest;

	protected AbstractLuaReadOperation(AbstractRedisClient client, String filename) {
		setName(ClassUtils.getShortName(getClass()));
		this.client = client;
		this.filename = filename;
	}

	@Override
	public synchronized void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (digest == null) {
			byte[] bytes;
			try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filename)) {
				bytes = FileCopyUtils.copyToByteArray(inputStream);
			} catch (IOException e) {
				throw new ItemStreamException("Could not load LUA script file " + filename);
			}
			try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
				RedisScriptingAsyncCommands<K, V> commands = Utils.async(connection);
				this.digest = commands.scriptLoad(bytes).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new ItemStreamException("Interrupted during initialization", e);
			} catch (Exception e) {
				throw new ItemStreamException("Could not initialize read operation", e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<T> execute(BaseRedisAsyncCommands<K, V> commands, K key) {
		RedisFuture<List<Object>> result = ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(digest,
				ScriptOutputType.MULTI, key);
		return new ConvertingRedisFuture<>(result, this::convert);
	}

	protected abstract T convert(List<Object> list);

	@Override
	public void close() {
		digest = null;
		super.close();
	}

}
