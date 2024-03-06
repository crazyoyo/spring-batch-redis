package com.redis.spring.batch.common;

import org.springframework.batch.item.Chunk;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Operation<K, V, I, O> {

	void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends I> inputs, Chunk<RedisFuture<O>> outputs);

}
