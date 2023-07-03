package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

public abstract class AbstractLuaReadOperation<K, V, T extends KeyValue<K>> implements Operation<K, V, K, T> {

	private final String digest;
	private MemoryUsageOptions memoryUsageOptions = MemoryUsageOptions.builder().build();

	protected AbstractLuaReadOperation(AbstractRedisClient client, String filename) {
		this.digest = Utils.loadScript(Utils.connectionSupplier(client), filename);
	}

	public MemoryUsageOptions getMemoryUsageOptions() {
		return memoryUsageOptions;
	}

	/**
	 * 
	 * @param limit memory limit in bytes
	 */
	public void setMemoryUsageOptions(MemoryUsageOptions options) {
		this.memoryUsageOptions = options;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Future<T> execute(BaseRedisAsyncCommands<K, V> commands, K key) {
		RedisFuture<List<Object>> result = eval((RedisScriptingAsyncCommands<K, V>) commands, key);
		return result.thenApply(this::convert).toCompletableFuture();
	}

	@SuppressWarnings("unchecked")
	private RedisFuture<List<Object>> eval(RedisScriptingAsyncCommands<K, V> commands, K key) {
		if (memoryUsageOptions.isEnabled()) {
			Object[] keys = { key };
			return commands.evalsha(digest, ScriptOutputType.MULTI, (K[]) keys,
					encodeValue(String.valueOf(memoryUsageOptions.getLimit().toBytes())),
					encodeValue(String.valueOf(memoryUsageOptions.getSamples())));
		}
		return commands.evalsha(digest, ScriptOutputType.MULTI, key);
	}

	protected abstract V encodeValue(String value);

	@SuppressWarnings("unchecked")
	private T convert(List<Object> list) {
		if (list == null) {
			return null;
		}
		T keyValue = keyValue();
		Iterator<Object> iterator = list.iterator();
		if (iterator.hasNext()) {
			keyValue.setKey((K) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setTtl((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setType(string(iterator.next()));
		}
		if (iterator.hasNext()) {
			keyValue.setMemoryUsage((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			setValue(keyValue, iterator.next());
		}
		return keyValue;
	}

	protected abstract void setValue(T keyValue, Object value);

	protected abstract String string(Object object);

	protected abstract T keyValue();

}
