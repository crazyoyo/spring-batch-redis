package com.redis.spring.batch.common;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Operation<K, V, I, O> {

    void execute(BaseRedisAsyncCommands<K, V> commands, I item, List<RedisFuture<O>> futures);

}
