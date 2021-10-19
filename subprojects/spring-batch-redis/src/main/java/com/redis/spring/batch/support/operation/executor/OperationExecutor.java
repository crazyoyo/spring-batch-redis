package com.redis.spring.batch.support.operation.executor;

import java.util.List;
import java.util.concurrent.Future;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

public interface OperationExecutor<K, V, T> {

	List<Future<?>> execute(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items);

}
