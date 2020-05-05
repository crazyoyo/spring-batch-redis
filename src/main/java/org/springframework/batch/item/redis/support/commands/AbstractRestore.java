package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.batch.item.redis.support.Command;
import org.springframework.batch.item.redis.support.KeyDump;

public abstract class AbstractRestore<K, V> implements Command<K, V, KeyDump<K>> {

    private static final long NO_EXPIRE_TTL = 0;

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, KeyDump<K> keyDump) {
        if (keyDump.exists()) {
            return restore((RedisKeyAsyncCommands<K, V>) commands, keyDump.getKey(), getTtl(keyDump), keyDump.getValue());
        }
        return ((RedisKeyAsyncCommands<K, V>) commands).del(keyDump.getKey());
    }

    protected abstract RedisFuture<?> restore(RedisKeyAsyncCommands<K, V> commands, K key, long ttl, byte[] value);

    /**
     * https://redis.io/commands/restore
     * If ttl is 0 the key is created without any expire, otherwise the specified expire time (in milliseconds) is set.
     */
    protected long getTtl(KeyDump<K> args) {
        if (args.hasTtl()) {
            return args.getPttl();
        }
        return NO_EXPIRE_TTL;
    }

}