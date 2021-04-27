package org.springframework.batch.item.redis.support;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.*;
import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.RedisOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.Map;

@SuppressWarnings("unchecked")
public class RedisOperations {


    public static <T> Eval.EvalBuilder<T> eval() {
        return Eval.builder();
    }

    public static <T> Expire.ExpireBuilder<T> expire() {
        return Expire.builder();
    }

    public static <T> Geoadd.GeoaddBuilder<T> geoadd() {
        return Geoadd.builder();
    }

    public static <T> Hset.HsetBuilder<T> hset() {
        return Hset.builder();
    }

    public static <T> Lpush.LpushBuilder<T> lpush() {
        return Lpush.builder();
    }

    public static <T> Noop<T> noop() {
        return new Noop<>();
    }

    public static <T> Restore.RestoreBuilder<T> restore() {
        return Restore.<T>builder();
    }

    public static <T> Rpush.RpushBuilder<T> rpush() {
        return Rpush.builder();
    }

    public static <T> Sadd.SaddBuilder<T> sadd() {
        return Sadd.builder();
    }

    public static <T> Set.SetBuilder<T> set() {
        return Set.builder();
    }

    public static <T> Xadd.XaddBuilder<T> xadd() {
        return Xadd.builder();
    }

    public static <T> Zadd.ZaddBuilder<T> zadd() {
        return Zadd.builder();
    }

    public static abstract class AbstractKeyOperation<T> implements RedisOperation<String, String, T> {

        protected final Converter<T, String> key;

        protected AbstractKeyOperation(Converter<T, String> key) {
            Assert.notNull(key, "A key converter is required");
            this.key = key;
        }

    }

    public static class KeyOperationBuilder<T, B extends KeyOperationBuilder<T,B>> {

        protected Converter<T, String> key;

        public B key(Converter<T, String> key) {
            this.key = key;
            return (B) this;
        }

    }

    public static abstract class AbstractCollectionOperation<T> extends AbstractKeyOperation<T> {

        protected final Converter<T, String> member;

        protected AbstractCollectionOperation(Converter<T, String> key, Converter<T, String> member) {
            super(key);
            Assert.notNull(member, "A member id converter is required");
            this.member = member;
        }

    }

    public static class CollectionOperationBuilder<T, B extends CollectionOperationBuilder<T,B>>  extends KeyOperationBuilder<T, B> {

        protected Converter<T, String> member;

        public B member(Converter<T, String> member) {
            this.member = member;
            return (B) this;
        }

    }

    @Setter
    @Accessors(fluent = true)
    public static class Eval<T> implements RedisOperation<String, String, T> {

        private final String sha;
        private final ScriptOutputType output;
        private final Converter<T, String[]> keys;
        private final Converter<T, String[]> args;

        @Builder
        public Eval(String sha, ScriptOutputType output, Converter<T, String[]> keys, Converter<T, String[]> args) {
            Assert.notNull(sha, "A SHA digest converter is required");
            Assert.notNull(output, "A script output type converter is required");
            Assert.notNull(keys, "A keys converter is required");
            Assert.notNull(args, "An args converter is required");
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
    public static class Expire<T> extends AbstractKeyOperation<T> {

        @NonNull
        private final Converter<T, Long> timeout;

        @Builder
        public Expire(Converter<T, String> key, Converter<T, Long> timeout) {
            super(key);
            Assert.notNull(timeout, "A timeout converter is required");
            this.timeout = timeout;
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
    public static class Hset<T> extends AbstractKeyOperation<T> {

        @NonNull
        private final Converter<T, Map<String, String>> map;

        @Builder
        public Hset(Converter<T, String> key, Converter<T, Map<String, String>> map) {
            super(key);
            Assert.notNull(map, "A map converter is required");
            this.map = map;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisHashAsyncCommands<String, String>) commands).hset(key.convert(item), map.convert(item));
        }
    }


    @Setter
    @Accessors(fluent = true)
    public static class Geoadd<T> extends AbstractCollectionOperation<T> {

        @NonNull
        private final Converter<T, Double> longitude;
        @NonNull
        private final Converter<T, Double> latitude;

        @Builder
        public Geoadd(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> longitude, Converter<T, Double> latitude) {
            super(key, member);
            Assert.notNull(longitude, "A longitude converter is required");
            Assert.notNull(latitude, "A latitude converter is required");
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

    public static class Lpush<T> extends AbstractCollectionOperation<T> {

        @Builder
        public Lpush(Converter<T, String> key, Converter<T, String> member) {
            super(key, member);
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisListAsyncCommands<String, String>) commands).lpush(key.convert(item), member.convert(item));
        }

    }

    public static class Noop<T> implements RedisOperation<String, String, T> {

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return null;
        }
    }

    public static class Restore<T> extends AbstractKeyOperation<T> {

        private final Converter<T, byte[]> dump;
        private final Converter<T, Long> absoluteTTL;
        private final Converter<T, Boolean> replace;

        public Restore(Converter<T, String> key, Converter<T, byte[]> dump, Converter<T, Long> absoluteTTL, Converter<T, Boolean> replace) {
            super(key);
            Assert.notNull(dump, "A dump converter is required");
            Assert.notNull(absoluteTTL, "A TTL converter is required");
            Assert.notNull(replace, "A replace converter is required");
            this.absoluteTTL = absoluteTTL;
            this.dump = dump;
            this.replace = replace;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            String key = this.key.convert(item);
            byte[] bytes = dump.convert(item);
            if (bytes == null) {
                return ((RedisKeyAsyncCommands<String, String>) commands).del(key);
            }
            Long ttl = this.absoluteTTL.convert(item);
            RestoreArgs restoreArgs = new RestoreArgs().replace(Boolean.TRUE.equals(replace.convert(item)));
            if (ttl != null && ttl > 0) {
                restoreArgs.absttl();
                restoreArgs.ttl(ttl);
            }
            return ((RedisKeyAsyncCommands<String, String>) commands).restore(key, bytes, restoreArgs);
        }

        public static <T> RestoreBuilder<T> builder() {
            return new RestoreBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class RestoreBuilder<T> extends KeyOperationBuilder<T, RestoreBuilder<T>> {

            private Converter<T, byte[]> dump;
            private Converter<T, Long> absoluteTTL;
            private Converter<T, Boolean> replace = t -> true;

            public Restore<T> build() {
                return new Restore<>(key, dump, absoluteTTL, replace);
            }
        }

    }


    public static class Rpush<T> extends AbstractCollectionOperation<T> {

        @Builder
        public Rpush(Converter<T, String> key, Converter<T, String> member) {
            super(key, member);
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisListAsyncCommands<String, String>) commands).rpush(key.convert(item), member.convert(item));
        }

    }

    public static class Sadd<T> extends AbstractCollectionOperation<T> {

        @Builder
        public Sadd(Converter<T, String> key, Converter<T, String> member) {
            super(key, member);
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisSetAsyncCommands<String, String>) commands).sadd(key.convert(item), member.convert(item));
        }

    }

    public static class Set<T> extends AbstractKeyOperation<T> {

        @NonNull
        private final Converter<T, String> value;

        @Builder
        public Set(Converter<T, String> key, Converter<T, String> value) {
            super(key);
            Assert.notNull(value, "A value converter is required");
            this.value = value;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisStringAsyncCommands<String, String>) commands).set(key.convert(item), value.convert(item));
        }

    }

    public static class Xadd<T> extends AbstractKeyOperation<T> {

        private final Converter<T, Map<String, String>> body;
        private final Converter<T, XAddArgs> args;

        public Xadd(Converter<T, String> key, Converter<T, Map<String, String>> body, Converter<T, XAddArgs> args) {
            super(key);
            Assert.notNull(body, "A body converter is required");
            Assert.notNull(args, "A XAddArgs converter is required");
            this.body = body;
            this.args = args;
        }

        @Override
        public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
            return ((RedisStreamAsyncCommands<String, String>) commands).xadd(key.convert(item), args.convert(item), body.convert(item));
        }

        public static <T> XaddBuilder<T> builder() {
            return new XaddBuilder<>();
        }

        @Setter
        @Accessors(fluent = true)
        public static class XaddBuilder<T> extends KeyOperationBuilder<T, XaddBuilder<T>> {

            private Converter<T, Map<String, String>> body;
            private Converter<T, XAddArgs> args = t -> null;

            public Xadd<T> build() {
                return new Xadd<>(key, body, args);
            }


        }

    }

    public static class Zadd<T> extends AbstractCollectionOperation<T> {

        private final Converter<T, Double> score;

        @Builder
        public Zadd(Converter<T, String> key, Converter<T, String> member, Converter<T, Double> score) {
            super(key, member);
            Assert.notNull(score, "A score converter is required");
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

}