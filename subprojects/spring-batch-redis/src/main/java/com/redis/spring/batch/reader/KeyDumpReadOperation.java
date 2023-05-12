package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class KeyDumpReadOperation<K, V> extends AbstractReadOperation<K, V, KeyDump<K>> {

	public KeyDumpReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec, PoolOptions poolOptions) {
		super(client, codec, poolOptions);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<KeyDump<K>> read(StatefulConnection<K, V> connection, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		List<RedisFuture<Long>> ttls = new ArrayList<>(keys.size());
		List<RedisFuture<byte[]>> dumps = new ArrayList<>(keys.size());
		List<KeyDump<K>> items = new ArrayList<>();
		for (K key : keys) {
			items.add(KeyDump.of(key));
			ttls.add(absoluteTTL(commands, key));
			dumps.add(((RedisKeyAsyncCommands<K, V>) commands).dump(key));
		}
		connection.flushCommands();
		for (int index = 0; index < items.size(); index++) {
			KeyDump<? extends K> item = items.get(index);
			byte[] dump = get(dumps.get(index), connection);
			item.setDump(dump);
			Long ttl = get(ttls.get(index), connection);
			item.setTtl(ttl);
		}
		return items;
	}

}
