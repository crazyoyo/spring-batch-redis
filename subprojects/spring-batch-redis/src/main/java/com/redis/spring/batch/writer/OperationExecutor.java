package com.redis.spring.batch.writer;

import java.util.List;
import java.util.concurrent.Future;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface OperationExecutor<K, V, T> {

	void execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items, List<Future<?>> futures);

}
