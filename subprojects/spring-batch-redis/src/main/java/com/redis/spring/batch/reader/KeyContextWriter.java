package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisServerAsyncCommands;

public class KeyContextWriter<K, V> extends AbstractItemStreamItemWriter<KeyContext<K>> {

	private final GenericObjectPool<StatefulConnection<K, V>> pool;

	public KeyContextWriter(GenericObjectPool<StatefulConnection<K, V>> pool) {
		Assert.notNull(pool, "Connection pool is required");
		setName(ClassUtils.getShortName(getClass()));
		this.pool = pool;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(List<? extends KeyContext<K>> keys) throws Exception {
		try (StatefulConnection<K, V> connection = pool.borrowObject()) {
			connection.setAutoFlushCommands(false);
			try {
				BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
				List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
				List<RedisFuture<Long>> memFutures = new ArrayList<>(keys.size());
				for (KeyContext<K> key : keys) {
					typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key.getKey()));
					memFutures.add(((RedisServerAsyncCommands<K, V>) commands).memoryUsage(key.getKey()));
				}
				connection.flushCommands();
				for (int index = 0; index < keys.size(); index++) {
					KeyContext<K> key = keys.get(index);
					String type = typeFutures.get(index).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
					Long mem = memFutures.get(index).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
					key.setType(type);
					if (mem != null) {
						key.setMemoryUsage(mem);
					}
				}
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
	}

}
