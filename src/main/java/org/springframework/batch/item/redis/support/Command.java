package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface Command<K, V, T> {

    RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T args);

}
