package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Restore<T> extends AbstractKeyOperation<T, byte[]> {

    private final Converter<T, Long> absoluteTTL;
    private final boolean replace;

    public Restore(Converter<T, String> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL) {
        this(key, value, absoluteTTL, true);
    }

    public Restore(Converter<T, String> key, Converter<T, byte[]> value, Converter<T, Long> absoluteTTL, boolean replace) {
        super(key, value, new NonexistentKeyPredicate<>(absoluteTTL));
        Assert.notNull(absoluteTTL, "A TTL converter is required");
        this.absoluteTTL = absoluteTTL;
        this.replace = replace;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, byte[] value) {
        Long ttl = this.absoluteTTL.convert(item);
        RestoreArgs restoreArgs = new RestoreArgs();
        restoreArgs.replace(replace);
        if (ttl != null && ttl > 0) {
            restoreArgs.absttl();
            restoreArgs.ttl(ttl);
        }
        return ((RedisKeyAsyncCommands<String, String>) commands).restore(key, value, restoreArgs);
    }

}
