package com.redis.spring.batch.writer.operation;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class DumpWriteOperation<K, V> implements WriteOperation<K, V, KeyValue<K>> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public RedisFuture<Object> execute(BaseRedisAsyncCommands<K, V> commands, KeyValue<K> item) {
        RedisKeyAsyncCommands<K, V> keyCommands = (RedisKeyAsyncCommands<K, V>) commands;
        if (item.getValue() == null || item.getTtl() == KeyValue.TTL_KEY_DOES_NOT_EXIST) {
            return (RedisFuture) keyCommands.del(item.getKey());
        }
        RestoreArgs args = new RestoreArgs().replace(true);
        if (item.getTtl() > 0) {
            args.absttl().ttl(item.getTtl());
        }
        return (RedisFuture) keyCommands.restore(item.getKey(), (byte[]) item.getValue(), args);
    }

}
