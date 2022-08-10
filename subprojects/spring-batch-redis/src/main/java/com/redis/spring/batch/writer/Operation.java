package com.redis.spring.batch.writer;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Operation<K, V, T> {

	RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item);

}
