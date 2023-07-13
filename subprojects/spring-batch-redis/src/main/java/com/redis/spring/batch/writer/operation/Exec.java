package com.redis.spring.batch.writer.operation;

import java.util.List;

import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisTransactionalAsyncCommands;

public class Exec<K, V, T> implements WriteOperation<K, V, T> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
		futures.add((RedisFuture) ((RedisTransactionalAsyncCommands<K, V>) commands).exec());
	}

}
