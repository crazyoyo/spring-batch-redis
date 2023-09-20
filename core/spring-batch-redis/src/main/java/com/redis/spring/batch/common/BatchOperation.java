package com.redis.spring.batch.common;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface BatchOperation<K, V, I, O> {

	List<RedisFuture<O>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items);

}