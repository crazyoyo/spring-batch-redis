package org.springframework.batch.item.redis.support.operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Restore<K, V, T> implements RedisOperation<K, V, T> {

    private final Converter<T, K> key;
    private final Converter<T, byte[]> dump;
    private final Converter<T, Long> absoluteTTL;
    private final Converter<T, Boolean> replace;

    public Restore(Converter<T, K> key, Converter<T, byte[]> dump, Converter<T, Long> absoluteTTL, Converter<T, Boolean> replace) {
        Assert.notNull(key, "A key converter is required");
        Assert.notNull(dump, "A dump converter is required");
        Assert.notNull(absoluteTTL, "A TTL converter is required");
        Assert.notNull(replace, "A replace converter is required");
        this.key = key;
        this.absoluteTTL = absoluteTTL;
        this.dump = dump;
        this.replace = replace;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
        K key = this.key.convert(item);
        byte[] bytes = dump.convert(item);
        if (bytes == null) {
            return ((RedisKeyAsyncCommands<K, V>) commands).del(key);
        }
        Long ttl = this.absoluteTTL.convert(item);
        RestoreArgs restoreArgs = new RestoreArgs().replace(Boolean.TRUE.equals(replace.convert(item)));
        if (ttl != null && ttl > 0) {
            restoreArgs.absttl();
            restoreArgs.ttl(ttl);
        }
        return ((RedisKeyAsyncCommands<K, V>) commands).restore(key, bytes, restoreArgs);
    }

    public static <T> RestoreBuilder<T> builder() {
        return new RestoreBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RestoreBuilder<T> extends AbstractKeyOperation.KeyOperationBuilder<T, RestoreBuilder<T>> {

        private Converter<T, byte[]> dump;
        private Converter<T, Long> absoluteTTL;
        private Converter<T, Boolean> replace = t -> true;

        public Restore<String, String, T> build() {
            return new Restore<>(key, dump, absoluteTTL, replace);
        }
    }

}