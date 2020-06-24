package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class CommandItemWriters {

    public static class Eval<K, V, T> extends AbstractCommandItemWriter<K, V, T> {

        private final String sha;
        private final ScriptOutputType outputType;
        private final Converter<T, K[]> keysConverter;
        private final Converter<T, V[]> argsConverter;

        public Eval(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, String sha, ScriptOutputType outputType, Converter<T, K[]> keysConverter, Converter<T, V[]> argsConverter) {
            super(pool, commands, commandTimeout);
            Assert.notNull(sha, "SHA is required.");
            Assert.notNull(outputType, "A script output type is required.");
            Assert.notNull(keysConverter, "Keys converter is required.");
            Assert.notNull(argsConverter, "Args converter is required.");
            this.sha = sha;
            this.outputType = outputType;
            this.keysConverter = keysConverter;
            this.argsConverter = argsConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
            return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, outputType, keysConverter.convert(item), argsConverter.convert(item));
        }

        public static <T> EvalBuilder<T> builder() {
            return new EvalBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class EvalBuilder<T> extends RedisConnectionBuilder<EvalBuilder<T>> {

            private String sha;
            private ScriptOutputType outputType;
            private Converter<T, String[]> keysConverter;
            private Converter<T, String[]> argsConverter;

            public Eval<String, String, T> build() {
                return new Eval<>(pool(), async(), timeout(), sha, outputType, keysConverter, argsConverter);
            }
        }
    }

    public static class Expire<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

        private final Converter<T, Long> timeoutConverter;

        public Expire(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, Long> timeoutConverter) {
            super(pool, commands, commandTimeout, keyConverter);
            Assert.notNull(timeoutConverter, "Timeout converter is required.");
            this.timeoutConverter = timeoutConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
            Long millis = timeoutConverter.convert(item);
            if (millis == null) {
                return null;
            }
            return ((RedisKeyAsyncCommands<K, V>) commands).pexpire(key, millis);
        }

        public static <T> ExpireBuilder<T> builder() {
            return new ExpireBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class ExpireBuilder<T> extends KeyCommandWriterBuilder<ExpireBuilder<T>, T> {

            private Converter<T, Long> timeoutConverter;

            public Expire<String, String, T> build() {
                return new Expire<>(pool(), async(), timeout(), keyConverter, timeoutConverter);
            }
        }

    }

    public static class Geoadd<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

        private final Converter<T, Double> longitudeConverter;
        private final Converter<T, Double> latitudeConverter;

        public Geoadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> longitudeConverter, Converter<T, Double> latitudeConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
            Assert.notNull(longitudeConverter, "Longitude converter is required.");
            Assert.notNull(latitudeConverter, "Latitude converter is required.");
            this.longitudeConverter = longitudeConverter;
            this.latitudeConverter = latitudeConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            Double longitude = longitudeConverter.convert(item);
            if (longitude == null) {
                return null;
            }
            Double latitude = latitudeConverter.convert(item);
            if (latitude == null) {
                return null;
            }
            return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, longitude, latitude, memberId);
        }

        public static <T> GeoaddBuilder<T> builder() {
            return new GeoaddBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class GeoaddBuilder<T> extends CollectionCommandWriterBuilder<GeoaddBuilder<T>, T> {

            private Converter<T, Double> longitudeConverter;
            private Converter<T, Double> latitudeConverter;

            public Geoadd<String, String, T> build() {
                return new Geoadd<>(pool(), async(), timeout(), keyConverter, memberIdConverter, longitudeConverter, latitudeConverter);
            }
        }
    }

    public static class Hmset<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

        private final Converter<T, Map<K, V>> mapConverter;

        public Hmset(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> mapConverter) {
            super(pool, commands, commandTimeout, keyConverter);
            Assert.notNull(mapConverter, "Map converter is required.");
            this.mapConverter = mapConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
            return ((RedisHashAsyncCommands<K, V>) commands).hmset(key, mapConverter.convert(item));
        }

        public static <T> HmsetBuilder<T> builder() {
            return new HmsetBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class HmsetBuilder<T> extends KeyCommandWriterBuilder<HmsetBuilder<T>, T> {

            private Converter<T, Map<String, String>> mapConverter;

            public Hmset<String, String, T> build() {
                return new Hmset<>(pool(), async(), timeout(), keyConverter, mapConverter);
            }
        }

    }

    public static class Lpush<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

        public Lpush(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            return ((RedisListAsyncCommands<K, V>) commands).lpush(key, memberId);
        }

        public static <T> LpushBuilder<T> builder() {
            return new LpushBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class LpushBuilder<T> extends CollectionCommandWriterBuilder<LpushBuilder<T>, T> {

            public Lpush<String, String, T> build() {
                return new Lpush<>(pool(), async(), timeout(), keyConverter, memberIdConverter);
            }
        }

    }

    public static class Rpush<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

        public Rpush(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            return ((RedisListAsyncCommands<K, V>) commands).rpush(key, memberId);
        }

        public static <T> RpushBuilder<T> builder() {
            return new RpushBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class RpushBuilder<T> extends CollectionCommandWriterBuilder<RpushBuilder<T>, T> {

            public Rpush<String, String, T> build() {
                return new Rpush<>(pool(), async(), timeout(), keyConverter, memberIdConverter);
            }
        }

    }

    public static class Noop<K, V, T> extends AbstractCommandItemWriter<K, V, T> {

        public Noop(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout) {
            super(pool, commands, commandTimeout);
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
            return null;
        }


        public static <T> NoopBuilder<T> builder() {
            return new NoopBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class NoopBuilder<T> extends RedisConnectionBuilder<NoopBuilder<T>> {

            public Noop<String, String, T> build() {
                return new Noop<>(pool(), async(), timeout());
            }
        }
    }

    public static class Sadd<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

        public Sadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, memberId);
        }

        public static <T> SaddBuilder<T> builder() {
            return new SaddBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class SaddBuilder<T> extends CollectionCommandWriterBuilder<SaddBuilder<T>, T> {

            public Sadd<String, String, T> build() {
                return new Sadd<>(pool(), async(), timeout(), keyConverter, memberIdConverter);
            }
        }

    }

    public static class Zadd<K, V, T> extends AbstractCollectionCommandItemWriter<K, V, T> {

        private final Converter<T, Double> scoreConverter;

        public Zadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> scoreConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
            Assert.notNull(scoreConverter, "Score converter is required.");
            this.scoreConverter = scoreConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            Double score = scoreConverter.convert(item);
            if (score == null) {
                return null;
            }
            return ((RedisSortedSetAsyncCommands<K, V>) commands).zadd(key, score, memberId);
        }

        public static <T> ZaddBuilder<T> builder() {
            return new ZaddBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class ZaddBuilder<T> extends CollectionCommandWriterBuilder<ZaddBuilder<T>, T> {

            private Converter<T, Double> scoreConverter;

            public Zadd<String, String, T> build() {
                return new Zadd<>(pool(), async(), timeout(), keyConverter, memberIdConverter, scoreConverter);
            }
        }

    }

    public static class Xadd<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

        private final Converter<T, Map<K, V>> bodyConverter;
        private final Converter<T, String> idConverter;
        private final Long maxlen;
        private final boolean approximateTrimming;

        public Xadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> bodyConverter, Converter<T, String> idConverter, Long maxlen, boolean approximateTrimming) {
            super(pool, commands, commandTimeout, keyConverter);
            Assert.notNull(bodyConverter, "Body converter is required.");
            this.bodyConverter = bodyConverter;
            this.idConverter = idConverter;
            this.maxlen = maxlen;
            this.approximateTrimming = approximateTrimming;
        }

        @Override
        protected RedisFuture<String> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
            XAddArgs args = new XAddArgs();
            if (idConverter != null) {
                args.id(idConverter.convert(item));
            }
            if (maxlen != null) {
                args.maxlen(maxlen);
            }
            args.approximateTrimming(approximateTrimming);
            return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args, bodyConverter.convert(item));
        }


        public static <T> XaddBuilder<T> builder() {
            return new XaddBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class XaddBuilder<T> extends KeyCommandWriterBuilder<XaddBuilder<T>, T> {

            private Converter<T, Map<String, String>> bodyConverter;
            private Converter<T, String> idConverter;
            private Long maxlen;
            private boolean approximateTrimming;

            public Xadd<String, String, T> build() {
                return new Xadd<>(pool(), async(), timeout(), keyConverter, bodyConverter, idConverter, maxlen, approximateTrimming);
            }
        }

    }

    public static class Set<K, V, T> extends AbstractKeyCommandItemWriter<K, V, T> {

        private final Converter<T, V> valueConverter;

        public Set(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> valueConverter) {
            super(pool, commands, commandTimeout, keyConverter);
            Assert.notNull(valueConverter, "Value converter is required.");
            this.valueConverter = valueConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
            return ((RedisStringAsyncCommands<K, V>) commands).set(key, valueConverter.convert(item));
        }


        public static <T> SetBuilder<T> builder() {
            return new SetBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class SetBuilder<T> extends KeyCommandWriterBuilder<SetBuilder<T>, T> {

            private Converter<T, String> valueConverter;

            public Set<String, String, T> build() {
                return new Set<>(pool(), async(), timeout(), keyConverter, valueConverter);
            }
        }
    }

    public static class KeyCommandWriterBuilder<B extends KeyCommandWriterBuilder<B, T>, T> extends RedisConnectionBuilder<B> {

        protected Converter<T, String> keyConverter;

        public B keyConverter(Converter<T, String> keyConverter) {
            this.keyConverter = keyConverter;
            return (B) this;
        }

    }

    public static class CollectionCommandWriterBuilder<B extends CollectionCommandWriterBuilder<B, T>, T> extends KeyCommandWriterBuilder<B, T> {

        protected Converter<T, String> memberIdConverter;

        public B memberIdConverter(Converter<T, String> memberIdConverter) {
            this.memberIdConverter = memberIdConverter;
            return (B) this;
        }

    }

}
