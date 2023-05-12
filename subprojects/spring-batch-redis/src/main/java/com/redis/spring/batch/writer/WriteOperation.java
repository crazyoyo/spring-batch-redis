package com.redis.spring.batch.writer;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface WriteOperation<K, V, T> {

	void execute(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures, T item);

}