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

    abstract class KeyCommandBuilder<K, V, C, T, B extends KeyCommandBuilder<K, V, C, T, B>> implements CommandBuilder<C, T> {

        @NonNull
        private Converter<T, K> keyConverter;

        @SuppressWarnings("unchecked")
        public B keyConverter(Converter<T, K> keyConverter) {
            this.keyConverter = keyConverter;
            return (B) this;
        }

        protected K key(T source) {
            return keyConverter.convert(source);
        }
    }

    abstract class CollectionCommandBuilder<K, V, C, T, B extends CollectionCommandBuilder<K, V, C, T, B>> extends KeyCommandBuilder<K, V, C, T, B> {

        @NonNull
        private Converter<T, V> memberIdConverter;

        @SuppressWarnings("unchecked")
        public B memberIdConverter(Converter<T, V> memberIdConverter) {
            this.memberIdConverter = memberIdConverter;
            return (B) this;
        }

        protected V memberId(T source) {
            return memberIdConverter.convert(source);
        }
    }

    @Setter
    @Accessors(fluent = true)
    class EvalBuilder<K, V, T> implements CommandBuilder<RedisScriptingAsyncCommands<K, V>, T> {

        @NonNull
        private String sha;
        @NonNull
        private ScriptOutputType outputType;
        @NonNull
        private Converter<T, K[]> keysConverter;
        @NonNull
        private Converter<T, V[]> argsConverter;

        public BiFunction<RedisScriptingAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.evalsha(sha, outputType, keysConverter.convert(t), argsConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class ExpireBuilder<K, V, T> extends KeyCommandBuilder<K, V, RedisKeyAsyncCommands<K, V>, T, ExpireBuilder<K, V, T>> {

        @NonNull
        private Converter<T, Long> timeoutConverter;

        public BiFunction<RedisKeyAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.expire(key(t), timeoutConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class HmsetBuilder<K, V, T> extends KeyCommandBuilder<K, V, RedisHashAsyncCommands<K, V>, T, HmsetBuilder<K, V, T>> {

        @NonNull
        private Converter<T, Map<K, V>> mapConverter;

        @Override
        public BiFunction<RedisHashAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.hmset(key(t), mapConverter.convert(t));
        }
    }


    @Setter
    @Accessors(fluent = true)
    class GeoaddBuilder<K, V, T> extends CollectionCommandBuilder<K, V, RedisGeoAsyncCommands<K, V>, T, GeoaddBuilder<K, V, T>> {

        @NonNull
        private Converter<T, Double> longitudeConverter;
        @NonNull
        private Converter<T, Double> latitudeConverter;

        public BiFunction<RedisGeoAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.geoadd(key(t), longitudeConverter.convert(t), latitudeConverter.convert(t), memberId(t));
        }

    }

    class LpushBuilder<K, V, T> extends CollectionCommandBuilder<K, V, RedisListAsyncCommands<K, V>, T, LpushBuilder<K, V, T>> {

        public BiFunction<RedisListAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.lpush(key(t), memberId(t));
        }

    }

    class NoopBuilder<K, V, T> implements CommandBuilder<BaseRedisAsyncCommands<K, V>, T> {

        @Override
        public BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> null;
        }
    }

    class RpushBuilder<K, V, T> extends CollectionCommandBuilder<K, V, RedisListAsyncCommands<K, V>, T, RpushBuilder<K, V, T>> {

        public BiFunction<RedisListAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.rpush(key(t), memberId(t));
        }

    }

    class SaddBuilder<K, V, T> extends CollectionCommandBuilder<K, V, RedisSetAsyncCommands<K, V>, T, SaddBuilder<K, V, T>> {

        public BiFunction<RedisSetAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.sadd(key(t), memberId(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class SetBuilder<K, V, T> extends KeyCommandBuilder<K, V, RedisStringAsyncCommands<K, V>, T, SetBuilder<K, V, T>> {

        @NonNull
        private Converter<T, V> valueConverter;

        public BiFunction<RedisStringAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.set(key(t), valueConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class ZaddBuilder<K, V, T> extends CollectionCommandBuilder<K, V, RedisSortedSetAsyncCommands<K, V>, T, ZaddBuilder<K, V, T>> {

        @NonNull
        private Converter<T, Double> scoreConverter;

        public BiFunction<RedisSortedSetAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.zadd(key(t), scoreConverter.convert(t), memberId(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class XaddBuilder<K, V, T> extends KeyCommandBuilder<K, V, RedisStreamAsyncCommands<K, V>, T, XaddBuilder<K, V, T>> {

        @NonNull
        private Converter<T, Map<K, V>> bodyConverter;
        @NonNull
        private Converter<T, XAddArgs> argsConverter = s -> null;

        @Override
        public BiFunction<RedisStreamAsyncCommands<K, V>, T, RedisFuture<?>> build() {
            return (c, t) -> c.xadd(key(t), argsConverter.convert(t), bodyConverter.convert(t));
        }
    }

    static <K, V, T> EvalBuilder<K, V, T> eval() {
        return new EvalBuilder<>();
    }

    static <K, V, T> ExpireBuilder<K, V, T> expire() {
        return new ExpireBuilder<>();
    }

    static <K, V, T> HmsetBuilder<K, V, T> hmset() {
        return new HmsetBuilder<>();
    }

    static <K, V, T> LpushBuilder<K, V, T> lpush() {
        return new LpushBuilder<>();
    }

    static <K, V, T> NoopBuilder<K, V, T> noop() {
        return new NoopBuilder<>();
    }

    static <K, V, T> RpushBuilder<K, V, T> rpush() {
        return new RpushBuilder<>();
    }

    static <K, V, T> SaddBuilder<K, V, T> sadd() {
        return new SaddBuilder<>();
    }

    static <K, V, T> SetBuilder<K, V, T> set() {
        return new SetBuilder<>();
    }

    static <K, V, T> XaddBuilder<K, V, T> xadd() {
        return new XaddBuilder<>();
    }

    static <K, V, T> ZaddBuilder<K, V, T> zadd() {
        return new ZaddBuilder<>();
    }

}
