package com.redis.spring.batch.support.operation.executor;

import java.util.List;
import java.util.concurrent.Future;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface OperationExecutor<K, V, T> {

	List<Future<?>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items);

}
