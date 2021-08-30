package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Restore<T> extends AbstractKeyOperation<T> {

    private final Converter<T, byte[]> value;
    private final Converter<T, Long> absoluteTTL;
    private final boolean replace;

    public Restore(Converter<T, Object> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL) {
        this(key, value, absoluteTTL, true);
    }

    public Restore(Converter<T, Object> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL, boolean replace) {
        super(key, new NonexistentKeyPredicate<>(absoluteTTL));
        Assert.notNull(value, "A value converter is required");
        Assert.notNull(absoluteTTL, "A TTL converter is required");
        this.value = value;
        this.absoluteTTL = absoluteTTL;
        this.replace = replace;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> RedisFuture<?> doExecute(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
        Long ttl = this.absoluteTTL.convert(item);
        RestoreArgs restoreArgs = new RestoreArgs();
        restoreArgs.replace(replace);
        if (ttl != null && ttl > 0) {
            restoreArgs.absttl();
            restoreArgs.ttl(ttl);
        }
        return ((RedisKeyAsyncCommands<K, V>) commands).restore(key, value.convert(item), restoreArgs);
    }

}
