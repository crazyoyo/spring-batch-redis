package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class Restore<K, V, T> extends AbstractOperation<K, V, T> {

    public static final long TTL_KEY_DOES_NOT_EXIST = -2;

    private Function<T, byte[]> bytes;

    private ToLongFunction<T> absoluteTtl;

    private Predicate<T> replace = Predicates.isFalse();

    public void setBytes(Function<T, byte[]> function) {
        this.bytes = function;
    }

    public void setTtl(ToLongFunction<T> function) {
        this.absoluteTtl = function;
    }

    public void setReplace(boolean replace) {
        this.replace = Predicates.is(replace);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, List<RedisFuture<Object>> futures) {
        byte[] dump = bytes.apply(item);
        long ttl = absoluteTtl.applyAsLong(item);
        if (dump == null || ttl == TTL_KEY_DOES_NOT_EXIST) {
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(key(item)));
        } else {
            RestoreArgs args = new RestoreArgs();
            args.replace(replace.test(item));
            if (ttl > 0) {
                args.absttl().ttl(ttl);
            }
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).restore(key(item), dump, args));
        }
    }

}
