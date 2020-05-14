package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class RestoreReplace<K, V> extends AbstractRestore<K, V> {

    @Override
    protected RedisFuture<?> restore(RedisKeyAsyncCommands<K, V> commands, K key, long ttl, byte[] value) {
        RestoreArgs restoreArgs = new RestoreArgs();
        restoreArgs.ttl(ttl);
        restoreArgs.replace(true);
        return commands.restore(key, value, restoreArgs);
    }

}