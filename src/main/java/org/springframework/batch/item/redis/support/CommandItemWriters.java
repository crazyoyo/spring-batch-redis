package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.*;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.core.convert.converter.Converter;

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
            this.sha = sha;
            this.outputType = outputType;
            this.keysConverter = keysConverter;
            this.argsConverter = argsConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
            return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, outputType, keysConverter.convert(item), argsConverter.convert(item));
        }
    }

    public static class Expire<K, V, T> extends AbstractKeyItemWriter<K, V, T> {

        private final Converter<T, Long> timeoutConverter;

        public Expire(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, Long> timeoutConverter) {
            super(pool, commands, commandTimeout, keyConverter);
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

    }

    public static class Geoadd<K, V, T> extends AbstractCollectionItemWriter<K, V, T> {

        private final Converter<T, Double> longitudeConverter;
        private final Converter<T, Double> latitudeConverter;

        public Geoadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> longitudeConverter, Converter<T, Double> latitudeConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
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
    }

    public static class Hmset<K, V, T> extends AbstractKeyItemWriter<K, V, T> {

        private final Converter<T, Map<K, V>> mapConverter;

        public Hmset(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> mapConverter) {
            super(pool, commands, commandTimeout, keyConverter);
            this.mapConverter = mapConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
            return ((RedisHashAsyncCommands<K, V>) commands).hmset(key, mapConverter.convert(item));
        }

    }

    public static class Lpush<K, V, T> extends AbstractCollectionItemWriter<K, V, T> {

        public Lpush(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            return ((RedisListAsyncCommands<K, V>) commands).lpush(key, memberId);
        }

    }

    public static class Rpush<K, V, T> extends AbstractCollectionItemWriter<K, V, T> {

        public Rpush(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            return ((RedisListAsyncCommands<K, V>) commands).rpush(key, memberId);
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
    }

    public static class Sadd<K, V, T> extends AbstractCollectionItemWriter<K, V, T> {

        public Sadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
            return ((RedisSetAsyncCommands<K, V>) commands).sadd(key, memberId);
        }

    }

    public static class Zadd<K, V, T> extends AbstractCollectionItemWriter<K, V, T> {

        private final Converter<T, Double> scoreConverter;

        public Zadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> memberIdConverter, Converter<T, Double> scoreConverter) {
            super(pool, commands, commandTimeout, keyConverter, memberIdConverter);
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

    }

    public static class Xadd<K, V, T> extends AbstractKeyItemWriter<K, V, T> {

        private final Converter<T, Map<K, V>> bodyConverter;
        private final Converter<T, String> idConverter;
        private final Long maxlen;
        private final boolean approximateTrimming;

        public Xadd(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, Map<K, V>> bodyConverter, Converter<T, String> idConverter, Long maxlen, boolean approximateTrimming) {
            super(pool, commands, commandTimeout, keyConverter);
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

    }

    public static class Set<K, V, T> extends AbstractKeyItemWriter<K, V, T> {

        private final Converter<T, V> valueConverter;

        public Set(GenericObjectPool<? extends StatefulConnection<K, V>> pool, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> commands, Duration commandTimeout, Converter<T, K> keyConverter, Converter<T, V> valueConverter) {
            super(pool, commands, commandTimeout, keyConverter);
            this.valueConverter = valueConverter;
        }

        @Override
        protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
            return ((RedisStringAsyncCommands<K, V>) commands).set(key, valueConverter.convert(item));
        }
    }

}
