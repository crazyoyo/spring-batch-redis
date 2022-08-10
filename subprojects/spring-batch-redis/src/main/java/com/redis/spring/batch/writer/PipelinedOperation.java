package com.redis.spring.batch.writer;

import java.util.Collection;
import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface PipelinedOperation<K, V, T> {

	Collection<RedisFuture<?>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items);

}