package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.spring.batch.KeyValue;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyDumpValueReader<K, V> extends AbstractValueReader<K, V, KeyValue<K, byte[]>> {

	public KeyDumpValueReader(Supplier<StatefulConnection<K, V>> connectionSupplier,
			GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig,
			Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async) {
		super(connectionSupplier, poolConfig, async);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<KeyValue<K, byte[]>> read(BaseRedisAsyncCommands<K, V> commands, long timeout,
			List<? extends K> keys) throws InterruptedException, ExecutionException, TimeoutException {
		List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
		List<RedisFuture<byte[]>> dumpFutures = new ArrayList<>(keys.size());
		for (K key : keys) {
			ttlFutures.add(absoluteTTL(commands, key));
			dumpFutures.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
		}
		commands.flushCommands();
		List<KeyValue<K, byte[]>> keyValueList = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			KeyValue<K, byte[]> keyValue = new KeyValue<>();
			keyValue.setKey(keys.get(index));
			keyValue.setValue(dumpFutures.get(index).get(timeout, TimeUnit.MILLISECONDS));
			keyValue.setTtl(ttlFutures.get(index).get(timeout, TimeUnit.MILLISECONDS));
			keyValueList.add(keyValue);
		}
		return keyValueList;
	}

}
