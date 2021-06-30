package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Restore<K, V, T> extends AbstractKeyOperation<K, V, T, byte[]> {

    private final Converter<T, Long> absoluteTTL;
    private final boolean replace;

    public Restore(Converter<T, K> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL) {
        this(key, value, absoluteTTL, true);
    }

    public Restore(Converter<T, K> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL, boolean replace) {
        super(key, value, new NonexistentKeyPredicate<>(absoluteTTL));
        Assert.notNull(absoluteTTL, "A TTL converter is required");
        this.absoluteTTL = absoluteTTL;
        this.replace = replace;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, byte[] value) {
        Long ttl = this.absoluteTTL.convert(item);
        RestoreArgs restoreArgs = new RestoreArgs();
        restoreArgs.replace(replace);
        if (ttl != null && ttl > 0) {
            restoreArgs.absttl();
            restoreArgs.ttl(ttl);
        }
        return ((RedisKeyAsyncCommands<K, V>) commands).restore(key, value, restoreArgs);
    }

}
