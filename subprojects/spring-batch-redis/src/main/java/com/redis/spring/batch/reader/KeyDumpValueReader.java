package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyDumpValueReader<K, V> extends AbstractValueReader<K, V, KeyDump<K>> {

	public KeyDumpValueReader(GenericObjectPool<StatefulConnection<K, V>> pool) {
		super(pool);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<KeyDump<K>> read(StatefulConnection<K, V> connection, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
		List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
		for (K key : keys) {
			ttlFutures.add(absoluteTTL(commands, key));
			dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
		}
		connection.flushCommands();
		List<KeyDump<K>> keyValueList = new ArrayList<>(keys.size());
		long timeout = connection.getTimeout().toMillis();
		for (int index = 0; index < keys.size(); index++) {
			KeyDump<K> keyValue = new KeyDump<>();
			keyValue.setKey(keys.get(index));
			keyValue.setDump(dumpFutures.get(index).get(timeout, TimeUnit.MILLISECONDS));
			keyValue.setTtl(ttlFutures.get(index).get(timeout, TimeUnit.MILLISECONDS));
			keyValueList.add(keyValue);
		}
		return keyValueList;
	}

}
