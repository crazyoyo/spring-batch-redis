package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;

public class KeyMetadataValueReader<K, V> extends AbstractValueReader<K, V, KeyMetadata<K>> {

	public KeyMetadataValueReader(GenericObjectPool<StatefulConnection<K, V>> pool) {
		super(pool);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<KeyMetadata<K>> read(StatefulConnection<K, V> connection, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
		List<RedisFuture<Long>> memFutures = new ArrayList<>(keys.size());
		for (K key : keys) {
			typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
			memFutures.add(((RedisServerAsyncCommands<K, V>) commands).memoryUsage(key));
		}
		connection.flushCommands();
		List<KeyMetadata<K>> metadatas = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			K key = keys.get(index);
			String type = typeFutures.get(index).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			Long mem = memFutures.get(index).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			KeyMetadata<K> metadata = new KeyMetadata<>();
			metadata.setKey(key);
			metadata.setType(type);
			metadata.setMemoryUsage(mem);
			metadatas.add(metadata);
		}
		return metadatas;
	}

}
