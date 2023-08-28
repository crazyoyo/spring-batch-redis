package com.redis.spring.batch.writer;

import java.util.List;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.MergePolicy;
import com.redis.spring.batch.RedisItemWriter.StreamIdPolicy;
import com.redis.spring.batch.RedisItemWriter.TtlPolicy;
import com.redis.spring.batch.writer.operation.Del;
import com.redis.spring.batch.writer.operation.ExpireAt;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Noop;
import com.redis.spring.batch.writer.operation.Restore;
import com.redis.spring.batch.writer.operation.RpushAll;
import com.redis.spring.batch.writer.operation.SaddAll;
import com.redis.spring.batch.writer.operation.Set;
import com.redis.spring.batch.writer.operation.TsAddAll;
import com.redis.spring.batch.writer.operation.XAddAll;
import com.redis.spring.batch.writer.operation.ZaddAll;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class StructOperation<K, V> implements Operation<K, V, KeyValue<K>> {

    private static final XAddArgs EMPTY_XADD_ARGS = new XAddArgs();

    private final ExpireAt<K, V, KeyValue<K>> expire = new ExpireAt<>(key(), KeyValue::getTtl);

    private final Noop<K, V, KeyValue<K>> noop = new Noop<>();

    private final Del<K, V, KeyValue<K>> del = new Del<>(key());

    private final Hset<K, V, KeyValue<K>> hset = new Hset<>(key(), value());

    private final Set<K, V, KeyValue<K>> set = new Set<>(key(), value());

    private final JsonSet<K, V, KeyValue<K>> jsonSet = new JsonSet<>(key(), value());

    private final RpushAll<K, V, KeyValue<K>> rpush = new RpushAll<>(key(), value());

    private final SaddAll<K, V, KeyValue<K>> sadd = new SaddAll<>(key(), value());

    private final ZaddAll<K, V, KeyValue<K>> zadd = new ZaddAll<>(key(), value());

    private final TsAddAll<K, V, KeyValue<K>> tsAdd = new TsAddAll<>(key(), value(), tsAddOptions());

    private final XAddAll<K, V, KeyValue<K>> xadd = new XAddAll<>(value(), this::xaddArgs);

    private TtlPolicy ttlPolicy = RedisItemWriter.DEFAULT_TTL_POLICY;

    private MergePolicy mergePolicy = RedisItemWriter.DEFAULT_MERGE_POLICY;

    private StreamIdPolicy streamIdPolicy = RedisItemWriter.DEFAULT_STREAM_ID_POLICY;

    public TtlPolicy getTtlPolicy() {
        return ttlPolicy;
    }

    public void setTtlPolicy(TtlPolicy ttlPolicy) {
        this.ttlPolicy = ttlPolicy;
    }

    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public void setMergePolicy(MergePolicy mergePolicy) {
        this.mergePolicy = mergePolicy;
    }

    public StreamIdPolicy getStreamIdPolicy() {
        return streamIdPolicy;
    }

    public void setStreamIdPolicy(StreamIdPolicy streamIdPolicy) {
        this.streamIdPolicy = streamIdPolicy;
    }

    private static <K, V> AddOptions<K, V> tsAddOptions() {
        AddOptions.Builder<K, V> builder = AddOptions.builder();
        return builder.build();
    }

    private static <K> Function<KeyValue<K>, K> key() {
        return KeyValue::getKey;
    }

    @SuppressWarnings("unchecked")
    private static <K, T> Function<KeyValue<K>, T> value() {
        return kv -> (T) kv.getValue();
    }

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, KeyValue<K> item, List<RedisFuture<Object>> futures) {
        if (shouldSkip(item)) {
            return;
        }
        if (shouldDelete(item)) {
            del.execute(commands, item, futures);
            return;
        }
        if (isOverwrite() && !KeyValue.isString(item)) {
            del.execute(commands, item, futures);
        }
        operation(item).execute(commands, item, futures);
        if (ttlPolicy == TtlPolicy.PROPAGATE && item.getTtl() > 0) {
            expire.execute(commands, item, futures);
        }
    }

    private boolean isOverwrite() {
        return mergePolicy == MergePolicy.OVERWRITE;
    }

    public static boolean shouldSkip(KeyValue<?> item) {
        return item == null || item.getKey() == null || (item.getValue() == null && item.getMemoryUsage() > 0);
    }

    private boolean shouldDelete(KeyValue<K> item) {
        return item.getValue() == null || Restore.TTL_KEY_DOES_NOT_EXIST == item.getTtl() || KeyValue.isNone(item);
    }

    private Operation<K, V, KeyValue<K>> operation(KeyValue<K> item) {
        switch (item.getType()) {
            case KeyValue.HASH:
                return hset;
            case KeyValue.JSON:
                return jsonSet;
            case KeyValue.LIST:
                return rpush;
            case KeyValue.SET:
                return sadd;
            case KeyValue.STREAM:
                return xadd;
            case KeyValue.STRING:
                return set;
            case KeyValue.TIMESERIES:
                return tsAdd;
            case KeyValue.ZSET:
                return zadd;
            default:
                return noop;
        }
    }

    private XAddArgs xaddArgs(StreamMessage<K, V> message) {
        if (streamIdPolicy == StreamIdPolicy.PROPAGATE) {
            XAddArgs args = new XAddArgs();
            String id = message.getId();
            if (id != null) {
                args.id(id);
            }
            return args;
        }
        return EMPTY_XADD_ARGS;
    }

}
