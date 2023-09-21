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

    private Function<T, byte[]> bytesFunction;

    private ToLongFunction<T> ttlFunction;

    private Predicate<T> replacePredicate = Predicates.isFalse();

    public void setBytesFunction(Function<T, byte[]> function) {
        this.bytesFunction = function;
    }

    public void setTtl(long ttl) {
        this.ttlFunction = t -> ttl;
    }

    public void setTtlFunction(ToLongFunction<T> function) {
        this.ttlFunction = function;
    }

    public void setReplace(boolean replace) {
        this.replacePredicate = Predicates.is(replace);
    }

    public void setReplacePredicate(Predicate<T> predicate) {
        this.replacePredicate = predicate;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, T item, K key, List<RedisFuture<Object>> futures) {
        byte[] dump = bytesFunction.apply(item);
        long ttl = ttlFunction.applyAsLong(item);
        if (dump == null || ttl == TTL_KEY_DOES_NOT_EXIST) {
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(key));
        } else {
            RestoreArgs args = new RestoreArgs();
            args.replace(replacePredicate.test(item));
            if (ttl > 0) {
                args.absttl().ttl(ttl);
            }
            futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).restore(key, dump, args));
        }
    }

}
