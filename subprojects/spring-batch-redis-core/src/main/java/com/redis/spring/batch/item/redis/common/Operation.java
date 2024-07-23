package com.redis.spring.batch.item.redis.common;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public interface Operation<K, V, I, O> {

	List<RedisFuture<O>> execute(RedisAsyncCommands<K, V> commands, List<? extends I> items);

}
