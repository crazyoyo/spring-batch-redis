package com.redis.spring.batch.common;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Operation<K, V, I, O> {

    RedisFuture<O> execute(BaseRedisAsyncCommands<K, V> commands, I item);

}
