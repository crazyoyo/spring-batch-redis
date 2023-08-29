package com.redis.spring.batch.writer;

import java.util.List;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Operation<K, V, T> {

    void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<?>> futures);

}
