package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.*;
import lombok.Builder;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisOperation<K, V, T> {

    RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item);

    abstract class AbstractKeyOperation<T> implements RedisOperation<String, String, T> {

        protected final Converter<T, String> key;

        protected AbstractKeyOperation(Converter<T, String> key) {
            this.key = key;
        }

    }

    abstract class AbstractCollectionOperation<T> extends AbstractKeyOperation<T> {

        protected final Converter<T, String> member;

        public AbstractCollectionOperation(Converter<T, String> key, Converter<T, String> member) {
            super(key);
            this.member = member;
        }

    }

    @Setter
    @Accessors(fluent = true)
    class EvalOperation<T> implements RedisOperation<String, String, T> {

        private final String sha;
        private final ScriptOutputType output;
        private final Converter<T, String[]> keys;
        private final Converter<T, String[]> args;

        @Builder
        public EvalOperation(String sha, ScriptOutputType output, Converter<T, String[]> keys, Converter<T, String[]> args) {
            this.sha = sha;
            this.output = output;
            this.keys = keys;
            this.args = args;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisScriptingAsyncCommands<String, String>) commands).evalsha(sha, output, keys.convert(item), args.convert(item));
        }

    }

    @Setter
    @Accessors(fluent = true)
    class ExpireOperation<T> extends AbstractKeyOperation<T> {

        private final Converter<T, Long> timeout;

        @Builder
        public ExpireOperation(Converter<T, String> key, Converter<T, Long> epochMillis) {
            super(key);
            this.timeout = epochMillis;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            Long timeout = this.timeout.convert(item);
            if (timeout == null) {
                return null;
            }
            return ((RedisKeyAsyncCommands<String, String>) commands).pexpire(key.convert(item), timeout);
        }

    }

    @Setter
    @Accessors(fluent = true)
    class HsetOperation<T> extends AbstractKeyOperation<T> {

        private final Converter<T, Map<String, String>> map;

        @Builder
        public HsetOperation(Converter<T, String> key, Converter<T, Map<String, String>> map) {
            super(key);
            this.map = map;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisHashAsyncCommands<String, String>) commands).hset(key.convert(item), map.convert(item));
        }
    }


    @Setter
    @Accessors(fluent = true)
    class GeoaddOperation<T> extends AbstractCollectionOperation<T> {

        private final Converter<T, Double> longitude;
        private final Converter<T, Double> latitude;

        @Builder
        public GeoaddOperation(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
            super(key, member);
            this.longitude = longitude;
            this.latitude = latitude;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            Double lon = longitude.convert(item);
            if (lon == null) {
                return null;
            }
            Double lat = latitude.convert(item);
            if (lat == null) {
                return null;
            }
            return ((RedisGeoAsyncCommands<String, String>) commands).geoadd(key.convert(item), lon, lat, member.convert(item));
        }

    }

    class LpushOperation<T> extends AbstractCollectionOperation<T> {

        @Builder
        public LpushOperation(Converter<T, String> key, Converter<T, String> member) {
            super(key, member);
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisListAsyncCommands<String, String>) commands).lpush(key.convert(item), member.convert(item));
        }

    }

    class NoopOperation<T> implements RedisOperation<String, String, T> {

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return null;
        }
    }

    class RpushOperation<T> extends AbstractCollectionOperation<T> {

        @Builder
        public RpushOperation(Converter<T, String> key, Converter<T, String> member) {
            super(key, member);
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisListAsyncCommands<String, String>) commands).rpush(key.convert(item), member.convert(item));
        }

    }

    class SaddOperation<T> extends AbstractCollectionOperation<T> {

        @Builder
        public SaddOperation(Converter<T, String> key, Converter<T, String> member) {
            super(key, member);
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisSetAsyncCommands<String, String>) commands).sadd(key.convert(item), member.convert(item));
        }

    }

    class SetOperation<T> extends AbstractKeyOperation<T> {

        private final Converter<T, String> value;

        @Builder
        public SetOperation(Converter<T, String> key, Converter<T, String> value) {
            super(key);
            this.value = value;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisStringAsyncCommands<String, String>) commands).set(key.convert(item), value.convert(item));
        }

    }

    class RestoreOperation<K, V, T> implements RedisOperation<K, V, T> {

        private final Converter<T, K> key;
        private final Converter<T, byte[]> dump;
        private final Converter<T, Long> ttl;
        private final Converter<T, Boolean> replace;

        @Builder
        public RestoreOperation(Converter<T, K> key, Converter<T, byte[]> dump, Converter<T, Long> absoluteTTL, Converter<T, Boolean> replace) {
            Assert.notNull(key, "A key converter is required");
            Assert.notNull(dump, "A dump converter is required");
            Assert.notNull(absoluteTTL, "A TTL converter is required");
            Assert.notNull(replace, "A replace converter is required");
            this.key = key;
            this.ttl = absoluteTTL;
            this.dump = dump;
            this.replace = replace;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<K, V> commands, T item) {
            K zeKey = key.convert(item);
            byte[] bytes = dump.convert(item);
            if (bytes == null) {
                return ((RedisKeyAsyncCommands<K, V>) commands).del(zeKey);
            }
            Long ttl = this.ttl.convert(item);
            RestoreArgs restoreArgs = new RestoreArgs().replace(Boolean.TRUE.equals(replace.convert(item)));
            if (ttl != null && ttl > 0) {
                restoreArgs.absttl();
                restoreArgs.ttl(ttl);
            }
            return ((RedisKeyAsyncCommands<K, V>) commands).restore(zeKey, bytes, restoreArgs);
        }
    }

    class ZaddBuilder<T> extends AbstractCollectionOperation<T> {

        private final Converter<T, Double> score;

        @Builder
        public ZaddBuilder(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> score) {
            super(key, member);
            this.score = score;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            Double scoreValue = score.convert(item);
            if (scoreValue == null) {
                return null;
            }
            return ((RedisSortedSetAsyncCommands<String, String>) commands).zadd(key.convert(item), scoreValue, member.convert(item));
        }

    }

    class XaddOperation<T> extends AbstractKeyOperation<T> {

        private final Converter<T, Map<String, String>> body;
        private Converter<T, XAddArgs> args = s -> null;

        @Builder
        public XaddOperation(Converter<T, String> key, Converter<T, Map<String, String>> body) {
            super(key);
            this.body = body;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisStreamAsyncCommands<String, String>) commands).xadd(key.convert(item), args.convert(item), body.convert(item));
        }
    }

}
