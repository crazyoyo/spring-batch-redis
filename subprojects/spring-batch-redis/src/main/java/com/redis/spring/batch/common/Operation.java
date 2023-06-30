package com.redis.spring.batch.common;

import java.util.concurrent.Future;

import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Operation<K, V, I, O> {

	Future<O> execute(BaseRedisAsyncCommands<K, V> commands, I item);

}
