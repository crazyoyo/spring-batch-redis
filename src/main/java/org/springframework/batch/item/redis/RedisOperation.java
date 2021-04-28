package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.redis.support.operation.*;

public interface RedisOperation<K, V, T> {

    RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item);

    static <T> Eval.EvalBuilder<T> eval() {
        return Eval.builder();
    }

    static <T> Expire.ExpireBuilder<T> expire() {
        return Expire.builder();
    }

    static <T> Geoadd.GeoaddBuilder<T> geoadd() {
        return Geoadd.builder();
    }

    static <T> Hset.HsetBuilder<T> hset() {
        return Hset.builder();
    }

    static <T> Lpush.LpushBuilder<T> lpush() {
        return Lpush.builder();
    }

    static <T> Noop<T> noop() {
        return new Noop<>();
    }

    static <T> Restore.RestoreBuilder<T> restore() {
        return Restore.<T>builder();
    }

    static <T> Rpush.RpushBuilder<T> rpush() {
        return Rpush.builder();
    }

    static <T> Sadd.SaddBuilder<T> sadd() {
        return Sadd.builder();
    }

    static <T> Set.SetBuilder<T> set() {
        return Set.builder();
    }

    static <T> Xadd.XaddBuilder<T> xadd() {
        return Xadd.builder();
    }

    static <T> Zadd.ZaddBuilder<T> zadd() {
        return Zadd.builder();
    }

}
