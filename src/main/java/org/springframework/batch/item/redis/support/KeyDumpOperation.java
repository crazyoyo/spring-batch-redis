package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class KeyDumpOperation<K, V> implements RedisOperation<K, V, KeyValue<K, byte[]>> {

    private final boolean replace;

    public KeyDumpOperation() {
        this(true);
    }

    public KeyDumpOperation(boolean replace) {
        this.replace = replace;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, KeyValue<K, byte[]> item) {
        if (item.getValue() == null || item.noKeyTtl()) {
            return ((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey());
        }
        RestoreArgs restoreArgs = new RestoreArgs().replace(replace);
        if (item.hasTtl()) {
            restoreArgs.ttl(item.getTtl() * 1000);
        }
        return ((RedisKeyAsyncCommands<K, V>) commands).restore(item.getKey(), item.getValue(), restoreArgs);

    }

}
