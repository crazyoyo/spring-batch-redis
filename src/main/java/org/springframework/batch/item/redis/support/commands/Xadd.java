package org.springframework.batch.item.redis.support.commands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import lombok.Builder;
import org.springframework.batch.item.redis.support.WriteCommand;

public class Xadd<K, V> implements WriteCommand<K, V, XaddArgs<K, V>> {

    @Override
    public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
        return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getFields());
    }

    @Builder
    public static class XaddId<K, V> implements WriteCommand<K, V, XaddArgs<K, V>> {

        @Override
        public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
            return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getId(), args.getFields());
        }

    }

    @Builder
    public static class XaddMaxlen<K, V> implements WriteCommand<K, V, XaddArgs<K, V>> {

        private final long maxlen;
        private final boolean approximateTrimming;

        @Override
        public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
            return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getFields(), maxlen, approximateTrimming);
        }

    }

    @Builder
    public static class XaddIdMaxlen<K, V> implements WriteCommand<K, V, XaddArgs<K, V>> {

        private final long maxlen;
        private final boolean approximateTrimming;

        @Override
        public RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, XaddArgs<K, V> args) {
            return ((RedisStreamAsyncCommands<K, V>) commands).xadd(args.getKey(), args.getId(), args.getFields(), maxlen, approximateTrimming);
        }

    }

}