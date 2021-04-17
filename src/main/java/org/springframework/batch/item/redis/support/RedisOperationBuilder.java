package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.*;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisOperationBuilder<K, V, T> {

    RedisOperation<K, V, T> build();

    abstract class AbstractKeyOperationBuilder<K, V, T, B extends AbstractKeyOperationBuilder<K, V, T, B>> implements RedisOperationBuilder<K, V, T> {

        private Converter<T, K> keyConverter;

        @SuppressWarnings("unchecked")
        public B keyConverter(Converter<T, K> keyConverter) {
            this.keyConverter = keyConverter;
            return (B) this;
        }

        @Override
        public RedisOperation<K, V, T> build() {
            return build(keyConverter);
        }

        protected abstract RedisOperation<K, V, T> build(Converter<T, K> keyConverter);

    }

    abstract class AbstractCollectionOperationBuilder<K, V, T, B extends AbstractCollectionOperationBuilder<K, V, T, B>> extends AbstractKeyOperationBuilder<K, V, T, B> {

        private Converter<T, V> memberIdConverter;

        public B memberIdConverter(Converter<T, V> memberIdConverter) {
            this.memberIdConverter = memberIdConverter;
            return (B) this;
        }

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter) {
            return build(keyConverter, memberIdConverter);
        }

        protected abstract RedisOperation<K, V, T> build(Converter<T, K> keyConverter, Converter<T, V> memberIdConverter);
    }

    @Setter
    @Accessors(fluent = true)
    class EvalBuilder<K, V, T> implements RedisOperationBuilder<K, V, T> {

        private String sha;
        private ScriptOutputType outputType;
        private Converter<T, K[]> keysConverter;
        private Converter<T, V[]> argsConverter;

        public RedisOperation<K, V, T> build() {
            return (c, t) -> ((RedisScriptingAsyncCommands<K, V>) c).evalsha(sha, outputType, keysConverter.convert(t), argsConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class ExpireBuilder<K, V, T> extends AbstractKeyOperationBuilder<K, V, T, ExpireBuilder<K, V, T>> {

        private Converter<T, Long> timeoutConverter;

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter) {
            return (c, t) -> {
                Long timeout = timeoutConverter.convert(t);
                if (timeout == null) {
                    return null;
                }
                return ((RedisKeyAsyncCommands<K, V>) c).expire(keyConverter.convert(t), timeout);
            };
        }

    }

    @Setter
    @Accessors(fluent = true)
    class HsetBuilder<K, V, T> extends AbstractKeyOperationBuilder<K, V, T, HsetBuilder<K, V, T>> {

        private Converter<T, Map<K, V>> mapConverter;

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter) {
            return (c, t) -> ((RedisHashAsyncCommands<K, V>) c).hset(keyConverter.convert(t), mapConverter.convert(t));
        }
    }


    @Setter
    @Accessors(fluent = true)
    class GeoaddBuilder<K, V, T> extends AbstractCollectionOperationBuilder<K, V, T, GeoaddBuilder<K, V, T>> {

        private Converter<T, Double> longitudeConverter;
        private Converter<T, Double> latitudeConverter;

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            return (c, t) -> {
                Double longitude = longitudeConverter.convert(t);
                if (longitude == null) {
                    return null;
                }
                Double latitude = latitudeConverter.convert(t);
                if (latitude == null) {
                    return null;
                }
                return ((RedisGeoAsyncCommands<K, V>) c).geoadd(keyConverter.convert(t), longitude, latitude, memberIdConverter.convert(t));
            };
        }

    }

    class LpushBuilder<K, V, T> extends AbstractCollectionOperationBuilder<K, V, T, LpushBuilder<K, V, T>> {

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            return (c, t) -> ((RedisListAsyncCommands<K, V>) c).lpush(keyConverter.convert(t), memberIdConverter.convert(t));
        }

    }

    class NoopBuilder<K, V, T> implements RedisOperationBuilder<K, V, T> {

        @Override
        public RedisOperation<K, V, T> build() {
            return (c, t) -> null;
        }
    }

    class RpushBuilder<K, V, T> extends AbstractCollectionOperationBuilder<K, V, T, RpushBuilder<K, V, T>> {

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            return (c, t) -> ((RedisListAsyncCommands<K, V>) c).rpush(keyConverter.convert(t), memberIdConverter.convert(t));
        }

    }

    class SaddBuilder<K, V, T> extends AbstractCollectionOperationBuilder<K, V, T, SaddBuilder<K, V, T>> {

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            return (c, t) -> ((RedisSetAsyncCommands<K, V>) c).sadd(keyConverter.convert(t), memberIdConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class SetBuilder<K, V, T> extends AbstractKeyOperationBuilder<K, V, T, SetBuilder<K, V, T>> {

        private Converter<T, V> valueConverter;

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter) {
            return (c, t) -> ((RedisStringAsyncCommands<K, V>) c).set(keyConverter.convert(t), valueConverter.convert(t));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class ZaddBuilder<K, V, T> extends AbstractCollectionOperationBuilder<K, V, T, ZaddBuilder<K, V, T>> {

        private Converter<T, Double> scoreConverter;

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            return (c, t) -> {
                Double score = scoreConverter.convert(t);
                if (score == null) {
                    return null;
                }
                return ((RedisSortedSetAsyncCommands<K, V>) c).zadd(keyConverter.convert(t), score, memberIdConverter.convert(t));
            };
        }

    }

    @Setter
    @Accessors(fluent = true)
    class XaddBuilder<K, V, T> extends AbstractKeyOperationBuilder<K, V, T, XaddBuilder<K, V, T>> {

        private Converter<T, Map<K, V>> bodyConverter;
        private Converter<T, XAddArgs> argsConverter = s -> null;

        @Override
        protected RedisOperation<K, V, T> build(Converter<T, K> keyConverter) {
            return (c, t) -> ((RedisStreamAsyncCommands<K, V>) c).xadd(keyConverter.convert(t), argsConverter.convert(t), bodyConverter.convert(t));
        }
    }

    static <K, V, T> EvalBuilder<K, V, T> eval() {
        return new EvalBuilder<>();
    }

    static <K, V, T> ExpireBuilder<K, V, T> expire() {
        return new ExpireBuilder<>();
    }

    static <K, V, T> GeoaddBuilder<K, V, T> geoadd() {
        return new GeoaddBuilder<>();
    }

    static <K, V, T> HsetBuilder<K, V, T> hset() {
        return new HsetBuilder<>();
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
