package org.springframework.batch.item.redis.support.operation.executor;

import java.util.List;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.RedisFuture;

public interface OperationExecutor<K, V, T> {

	List<RedisFuture<?>> execute(RedisModulesAsyncCommands<K, V> commands, List<? extends T> items);

}
