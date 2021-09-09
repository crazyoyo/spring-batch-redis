package org.springframework.batch.item.redis;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.redis.support.AbstractPipelineItemWriter;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.TransactionalOperationItemWriter;
import org.springframework.batch.item.redis.support.operation.Hset;
import org.springframework.batch.item.redis.support.operation.JsonSet;
import org.springframework.batch.item.redis.support.operation.Sadd;
import org.springframework.batch.item.redis.support.operation.TsAdd;
import org.springframework.batch.item.redis.support.operation.Xadd;
import org.springframework.batch.item.redis.support.operation.Zadd;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class OperationItemWriter<K, V, T> extends AbstractPipelineItemWriter<K, V, T> {

    private final RedisOperation<K, V, T> operation;

    public OperationItemWriter(Supplier<StatefulConnection<K, V>> connectionSupplier, GenericObjectPoolConfig<StatefulConnection<K, V>> poolConfig, Function<StatefulConnection<K, V>, BaseRedisAsyncCommands<K, V>> async, RedisOperation<K, V, T> operation) {
        super(connectionSupplier, poolConfig, async);
        Assert.notNull(operation, "A Redis operation is required");
        this.operation = operation;
    }

    @Override
    protected void write(BaseRedisAsyncCommands<K, V> commands, long timeout, List<? extends T> items) {
        List<RedisFuture<?>> futures = write(commands, items);
        commands.flushCommands();
        LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures.toArray(new RedisFuture[0]));
    }

    protected List<RedisFuture<?>> write(BaseRedisAsyncCommands<K, V> commands, List<? extends T> items) {
        List<RedisFuture<?>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            futures.add(operation.execute(commands, item));
        }
        return futures;
    }

    public static <T> HsetBuilder<String, String, T> hset() {
        return hset(StringCodec.UTF8);
    }

    public static <K, V, T> HsetBuilder<K, V, T> hset(RedisCodec<K, V> codec) {
        return new HsetBuilder<>(codec);
    }

    public static <T> SaddBuilder<String, String, T> sadd() {
        return sadd(StringCodec.UTF8);
    }

    public static <K, V, T> SaddBuilder<K, V, T> sadd(RedisCodec<K, V> codec) {
        return new SaddBuilder<>(codec);
    }

    public static <T> XaddBuilder<String, String, T> xadd() {
        return xadd(StringCodec.UTF8);
    }

    public static <K, V, T> XaddBuilder<K, V, T> xadd(RedisCodec<K, V> codec) {
        return new XaddBuilder<>(codec);
    }

    public static <T> ZaddBuilder<String, String, T> zadd() {
        return zadd(StringCodec.UTF8);
    }

    public static <K, V, T> ZaddBuilder<K, V, T> zadd(RedisCodec<K, V> codec) {
        return new ZaddBuilder<>(codec);
    }

    public static <T> JsonSetBuilder<String, String, T> jsonSet() {
        return jsonSet(StringCodec.UTF8);
    }

    public static <K, V, T> JsonSetBuilder<K, V, T> jsonSet(RedisCodec<K, V> codec) {
        return new JsonSetBuilder<>(codec);
    }

    public static <T> TsAddBuilder<String, String, T> tsAdd() {
        return tsAdd(StringCodec.UTF8);
    }

    public static <K, V, T> TsAddBuilder<K, V, T> tsAdd(RedisCodec<K, V> codec) {
        return new TsAddBuilder<>(codec);
    }


    public static class JsonSetBuilder<K, V, T> {
        private final RedisCodec<K, V> codec;

        public JsonSetBuilder(RedisCodec<K, V> codec) {
            this.codec = codec;
        }

        public PathJsonSetBuilder<K, V, T> key(Converter<T, K> key) {
            return new PathJsonSetBuilder<>(codec, key);
        }

    }

    public static class PathJsonSetBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;

        public PathJsonSetBuilder(RedisCodec<K, V> codec, Converter<T, K> key) {
            this.codec = codec;
            this.key = key;
        }

        public ValueJsonSetBuilder<K, V, T> path(Converter<T, K> path) {
            return new ValueJsonSetBuilder<>(codec, key, path);
        }

        public ValueJsonSetBuilder<K, V, T> path(K path) {
            return new ValueJsonSetBuilder<>(codec, key, t -> path);
        }

    }

    public static class ValueJsonSetBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;
        private final Converter<T, K> path;

        public ValueJsonSetBuilder(RedisCodec<K, V> codec, Converter<T, K> key, Converter<T, K> path) {
            this.codec = codec;
            this.key = key;
            this.path = path;
        }

        public ClientOperationItemWriterBuilder<K, V, T> value(Converter<T, V> value) {
            return new ClientOperationItemWriterBuilder<>(codec, new JsonSet<>(key, new NullValuePredicate<>(value), path, value));
        }

    }

    public static class SaddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;

        public SaddBuilder(RedisCodec<K, V> codec) {
            this.codec = codec;
        }

        public MemberSaddBuilder<K, V, T> key(Converter<T, K> key) {
            return new MemberSaddBuilder<>(codec, key);
        }

        public MemberSaddBuilder<K, V, T> key(K key) {
            return new MemberSaddBuilder<>(codec, t -> key);
        }

    }

    public static class ZaddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;

        public ZaddBuilder(RedisCodec<K, V> codec) {
            this.codec = codec;
        }

        public ScoreZaddBuilder<K, V, T> key(Converter<T, K> key) {
            return new ScoreZaddBuilder<>(codec, key);
        }

        public ScoreZaddBuilder<K, V, T> key(K key) {
            return new ScoreZaddBuilder<>(codec, t -> key);
        }

    }

    public static class ScoreZaddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;

        public ScoreZaddBuilder(RedisCodec<K, V> codec, Converter<T, K> key) {
            this.codec = codec;
            this.key = key;
        }

        public MemberZaddBuilder<K, V, T> score(Converter<T, Double> score) {
            return new MemberZaddBuilder<>(codec, key, score);
        }

    }

    public static class MemberZaddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;
        private final Converter<T, Double> score;

        public MemberZaddBuilder(RedisCodec<K, V> codec, Converter<T, K> key, Converter<T, Double> score) {
            this.codec = codec;
            this.key = key;
            this.score = score;
        }

        public ClientOperationItemWriterBuilder<K, V, T> member(Converter<T, V> member) {
            return new ClientOperationItemWriterBuilder<>(codec, new Zadd<>(key, t -> false, member, t -> false, score));
        }

    }

    public static class MemberSaddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;

        public MemberSaddBuilder(RedisCodec<K, V> codec, Converter<T, K> key) {
            this.codec = codec;
            this.key = key;
        }

        public ClientOperationItemWriterBuilder<K, V, T> member(Converter<T, V> member) {
            return new ClientOperationItemWriterBuilder<>(codec, new Sadd<>(key, t -> false, member, t -> false));
        }

    }

    public static class XaddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;

        public XaddBuilder(RedisCodec<K, V> codec) {
            this.codec = codec;
        }

        public BodyXaddBuilder<K, V, T> key(K key) {
            return key(t -> key);
        }

        public BodyXaddBuilder<K, V, T> key(Converter<T, K> key) {
            return new BodyXaddBuilder<>(codec, key);
        }

    }

    @Setter
    @Accessors(fluent = true)
    public static class BodyXaddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;
        private XAddArgs args = new XAddArgs();

        public BodyXaddBuilder(RedisCodec<K, V> codec, Converter<T, K> key) {
            this.codec = codec;
            this.key = key;
        }

        public ClientOperationItemWriterBuilder<K, V, T> body(Converter<T, Map<K, V>> body) {
            return new ClientOperationItemWriterBuilder<>(codec, new Xadd<>(key, new NullValuePredicate<>(body), body, t -> args));
        }

    }

    public static class HsetBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;

        public HsetBuilder(RedisCodec<K, V> codec) {
            this.codec = codec;
        }

        public MapHsetBuilder<K, V, T> key(Converter<T, K> key) {
            return new MapHsetBuilder<>(codec, key);
        }
    }

    public static class MapHsetBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;

        public MapHsetBuilder(RedisCodec<K, V> codec, Converter<T, K> key) {
            this.codec = codec;
            this.key = key;
        }

        public ClientOperationItemWriterBuilder<K, V, T> map(Converter<T, Map<K, V>> map) {
            return new ClientOperationItemWriterBuilder<>(codec, new Hset<>(key, new NullValuePredicate<>(map), map));
        }

    }

    public static class TsAddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;

        public TsAddBuilder(RedisCodec<K, V> codec) {
            this.codec = codec;
        }

        public TimestampTsAddBuilder<K, V, T> key(Converter<T, K> key) {
            return new TimestampTsAddBuilder<>(codec, key);
        }

        public TimestampTsAddBuilder<K, V, T> key(K key) {
            return new TimestampTsAddBuilder<>(codec, t -> key);
        }

    }

    public static class TimestampTsAddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;

        public TimestampTsAddBuilder(RedisCodec<K, V> codec, Converter<T, K> key) {
            this.codec = codec;
            this.key = key;
        }

        public ValueTsAddBuilder<K, V, T> timestamp(Converter<T, Long> timestamp) {
            return new ValueTsAddBuilder<>(codec, key, timestamp);
        }

    }

    public static class ValueTsAddBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final Converter<T, K> key;
        private final Converter<T, Long> timestamp;

        public ValueTsAddBuilder(RedisCodec<K, V> codec, Converter<T, K> key, Converter<T, Long> timestamp) {
            this.codec = codec;
            this.key = key;
            this.timestamp = timestamp;
        }

        public ClientOperationItemWriterBuilder<K, V, T> value(Converter<T, Double> value) {
            return new ClientOperationItemWriterBuilder<>(codec, new TsAdd<>(key, t -> false, timestamp, value));
        }

    }

    private static class NullValuePredicate<T> implements Predicate<T> {

        private final Converter<T, ?> value;

        public NullValuePredicate(Converter<T, ?> value) {
            Assert.notNull(value, "A value converter is required");
            this.value = value;
        }

        @Override
        public boolean test(T t) {
            return value.convert(t) == null;
        }

    }

    public static class ClientOperationItemWriterBuilder<K, V, T> {

        private final RedisCodec<K, V> codec;
        private final RedisOperation<K, V, T> operation;

        public ClientOperationItemWriterBuilder(RedisCodec<K, V> codec, RedisOperation<K, V, T> operation) {
            this.codec = codec;
            this.operation = operation;
        }

        public OperationItemWriterBuilder<K, V, T> client(AbstractRedisClient client) {
            return new OperationItemWriterBuilder<>(client, codec, operation);
        }

    }

    public static class OperationItemWriterBuilder<K, V, T> extends CommandBuilder<K, V, OperationItemWriterBuilder<K, V, T>> {

        private final RedisOperation<K, V, T> operation;
        private boolean transactional;

        public OperationItemWriterBuilder<K, V, T> transactional() {
            this.transactional = true;
            return this;
        }

        public OperationItemWriterBuilder(AbstractRedisClient client, RedisCodec<K, V> codec, RedisOperation<K, V, T> operation) {
            super(client, codec);
            this.operation = operation;
        }

        public OperationItemWriter<K, V, T> build() {
            if (transactional) {
                return new TransactionalOperationItemWriter<>(connectionSupplier(), poolConfig, async(), operation);
            }
            return new OperationItemWriter<>(connectionSupplier(), poolConfig, async(), operation);
        }

    }

}
