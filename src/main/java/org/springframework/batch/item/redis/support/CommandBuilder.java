package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.*;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;
import java.util.function.BiFunction;

public interface CommandBuilder<C, T> {

    BiFunction<C, T, RedisFuture<?>> build();

    abstract class KeyCommandBuilder<C, T, B extends KeyCommandBuilder<C, T, B>> implements CommandBuilder<C, T> {

        @NonNull
        private Converter<T, String> keyConverter;

        @SuppressWarnings("unchecked")
        public B keyConverter(Converter<T, String> keyConverter) {
            this.keyConverter = keyConverter;
            return (B) this;
        }

        protected String key(T source) {
            return keyConverter.convert(source);
        }
    }

    abstract class CollectionCommandBuilder<C, T, B extends CollectionCommandBuilder<C, T, B>> extends KeyCommandBuilder<C, T, B> {

        @NonNull
        private Converter<T, String> memberIdConverter;

        @SuppressWarnings("unchecked")
        public B memberIdConverter(Converter<T, String> memberIdConverter) {
            this.memberIdConverter = memberIdConverter;
            return (B) this;
        }

        protected String memberId(T source) {
            return memberIdConverter.convert(source);
        }
    }

    @Setter
    @Accessors(fluent = true)
    class EvalBuilder<T> implements CommandBuilder<RedisScriptingAsyncCommands<String, String>, T> {

        @NonNull
        private String sha;
        @NonNull
        private ScriptOutputType outputType;
        @NonNull
        private Converter<T, String[]> keysConverter;
        @NonNull
        private Converter<T, String[]> argsConverter;

        public BiFunction<RedisScriptingAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.evalsha(sha, outputType, keysConverter.convert(t), argsConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class ExpireBuilder<T> extends KeyCommandBuilder<RedisKeyAsyncCommands<String, String>, T, ExpireBuilder<T>> {

        @NonNull
        private Converter<T, Long> timeoutConverter;

        public BiFunction<RedisKeyAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.expire(key(t), timeoutConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class HmsetBuilder<T> extends KeyCommandBuilder<RedisHashAsyncCommands<String, String>, T, HmsetBuilder<T>> {

        @NonNull
        private Converter<T, Map<String, String>> mapConverter;

        @Override
        public BiFunction<RedisHashAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.hmset(key(t), mapConverter.convert(t));
        }
    }


    @Setter
    @Accessors(fluent = true)
    class GeoaddBuilder<T> extends CollectionCommandBuilder<RedisGeoAsyncCommands<String, String>, T, GeoaddBuilder<T>> {

        @NonNull
        private Converter<T, Double> longitudeConverter;
        @NonNull
        private Converter<T, Double> latitudeConverter;

        public BiFunction<RedisGeoAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.geoadd(key(t), longitudeConverter.convert(t), latitudeConverter.convert(t), memberId(t));
        }

    }

    class LpushBuilder<T> extends CollectionCommandBuilder<RedisListAsyncCommands<String, String>, T, LpushBuilder<T>> {

        public BiFunction<RedisListAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.lpush(key(t), memberId(t));
        }

    }

    class NoopBuilder<T> implements CommandBuilder<BaseRedisAsyncCommands<String, String>, T> {

        @Override
        public BiFunction<BaseRedisAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> null;
        }
    }

    class RpushBuilder<T> extends CollectionCommandBuilder<RedisListAsyncCommands<String, String>, T, RpushBuilder<T>> {

        public BiFunction<RedisListAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.rpush(key(t), memberId(t));
        }

    }

    class SaddBuilder<T> extends CollectionCommandBuilder<RedisSetAsyncCommands<String, String>, T, SaddBuilder<T>> {

        public BiFunction<RedisSetAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.sadd(key(t), memberId(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class SetBuilder<T> extends KeyCommandBuilder<RedisStringAsyncCommands<String, String>, T, SetBuilder<T>> {

        @NonNull
        private Converter<T, String> valueConverter;

        public BiFunction<RedisStringAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.set(key(t), valueConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class ZaddBuilder<T> extends CollectionCommandBuilder<RedisSortedSetAsyncCommands<String, String>, T, ZaddBuilder<T>> {

        @NonNull
        private Converter<T, Double> scoreConverter;

        public BiFunction<RedisSortedSetAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.zadd(key(t), scoreConverter.convert(t), memberId(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class XaddBuilder<T> extends KeyCommandBuilder<RedisStreamAsyncCommands<String, String>, T, XaddBuilder<T>> {

        @NonNull
        private Converter<T, Map<String, String>> bodyConverter;
        @NonNull
        private Converter<T, XAddArgs> argsConverter = s -> null;

        @Override
        public BiFunction<RedisStreamAsyncCommands<String, String>, T, RedisFuture<?>> build() {
            return (c, t) -> c.xadd(key(t), argsConverter.convert(t), bodyConverter.convert(t));
        }
    }

    static <T> EvalBuilder<T> eval() {
        return new EvalBuilder<>();
    }

    static <T> ExpireBuilder<T> expire() {
        return new ExpireBuilder<>();
    }

    static <T> HmsetBuilder<T> hmset() {
        return new HmsetBuilder<>();
    }

    static <T> LpushBuilder<T> lpush() {
        return new LpushBuilder<>();
    }

    static <T> NoopBuilder<T> noop() {
        return new NoopBuilder<>();
    }

    static <T> RpushBuilder<T> rpush() {
        return new RpushBuilder<>();
    }

    static <T> SaddBuilder<T> sadd() {
        return new SaddBuilder<>();
    }

    static <T> SetBuilder<T> set() {
        return new SetBuilder<>();
    }

    static <T> XaddBuilder<T> xadd() {
        return new XaddBuilder<>();
    }

    static <T> ZaddBuilder<T> zadd() {
        return new ZaddBuilder<>();
    }

}
