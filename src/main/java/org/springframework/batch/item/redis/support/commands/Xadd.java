package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import org.springframework.batch.item.redis.support.Command;

public class Xadd<K, V> implements Command<K, V, XaddArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
        return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getFields());
    }

    public static class XaddId<K, V> implements Command<K, V, XaddArgs<K, V>> {

        @Override
        public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
            return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getId(), args.getFields());
        }

    }

    public static class XaddMaxlen<K, V> implements Command<K, V, XaddArgs<K, V>> {

        @Override
        public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
            return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getFields(), args.getMaxlen(), args.isApproximateTrimming());
        }

    }

    public static class XaddIdMaxlen<K, V> implements Command<K, V, XaddArgs<K, V>> {

        @Override
        public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
            return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getId(), args.getFields(), args.getMaxlen(), args.isApproximateTrimming());
        }

    }

}