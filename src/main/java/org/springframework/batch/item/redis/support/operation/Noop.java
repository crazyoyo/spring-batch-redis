package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.redis.RedisOperation;

public class Noop<T> implements RedisOperation<String, String, T> {

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return null;
        }
    }