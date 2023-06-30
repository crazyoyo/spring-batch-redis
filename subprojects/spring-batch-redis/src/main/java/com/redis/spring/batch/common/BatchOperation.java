package com.redis.spring.batch.common;

import java.util.List;
import java.util.concurrent.Future;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface BatchOperation<K, V, I, O> {

	List<Future<O>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends I> items);

}
