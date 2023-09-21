package com.redis.spring.batch.writer;

import java.util.List;

import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.writer.operation.Restore;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class DumpWriteOperation<K, V> implements Operation<K, V, Dump<K>, Object> {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, Dump<K> item, List<RedisFuture<Object>> futures) {
        RedisKeyAsyncCommands<K, V> keyCommands = (RedisKeyAsyncCommands<K, V>) commands;
        futures.add((RedisFuture) execute(keyCommands, item));
    }

    @SuppressWarnings("unchecked")
    private RedisFuture<?> execute(RedisKeyAsyncCommands<K, V> commands, Dump<K> item) {
        if (isDel(item)) {
            return commands.del(item.getKey());
        }
        RestoreArgs args = new RestoreArgs().replace(true);
        if (item.getTtl() > 0) {
            args.absttl().ttl(item.getTtl());
        }
        return commands.restore(item.getKey(), item.getValue(), args);
    }

    private boolean isDel(Dump<K> item) {
        return item.getValue() == null || item.getTtl() == Restore.TTL_KEY_DOES_NOT_EXIST;
    }

}
