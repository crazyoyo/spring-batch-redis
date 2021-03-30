package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public interface RedisOperation<K, V, T> {

    RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item);

}
