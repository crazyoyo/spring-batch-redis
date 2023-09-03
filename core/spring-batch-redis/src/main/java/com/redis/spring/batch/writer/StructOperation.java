package com.redis.spring.batch.writer;

import java.util.List;
import java.util.function.Function;

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

    private final ExpireAt<K, V, KeyValue<K>> expire = expire();

    private final Noop<K, V, KeyValue<K>> noop = new Noop<>();

    private final Del<K, V, KeyValue<K>> del = del();

    private final Hset<K, V, KeyValue<K>> hset = hset();

    private final Set<K, V, KeyValue<K>> set = set();

    private final JsonSet<K, V, KeyValue<K>> jsonSet = jsonSet();

    private final RpushAll<K, V, KeyValue<K>> rpush = rpushAll();

    private final SaddAll<K, V, KeyValue<K>> sadd = saddAll();

    private final ZaddAll<K, V, KeyValue<K>> zadd = zaddAll();

    private final TsAddAll<K, V, KeyValue<K>> tsAdd = tsAddAll();

    private final XAddAll<K, V, KeyValue<K>> xadd = xaddAll();

    private TtlPolicy ttlPolicy = RedisItemWriter.DEFAULT_TTL_POLICY;

    private MergePolicy mergePolicy = RedisItemWriter.DEFAULT_MERGE_POLICY;

    private StreamIdPolicy streamIdPolicy = RedisItemWriter.DEFAULT_STREAM_ID_POLICY;

    public StructOperation<K, V> ttlPolicy(TtlPolicy ttlPolicy) {
        this.ttlPolicy = ttlPolicy;
        return this;
    }

    private XAddAll<K, V, KeyValue<K>> xaddAll() {
        XAddAll<K, V, KeyValue<K>> operation = new XAddAll<>();
        operation.setMessages(value());
        operation.setArgs(this::xaddArgs);
        return operation;
    }

    private TsAddAll<K, V, KeyValue<K>> tsAddAll() {
        TsAddAll<K, V, KeyValue<K>> operation = new TsAddAll<>();
        operation.setKey(key());
        operation.setSamples(value());
        return operation;
    }

    private ZaddAll<K, V, KeyValue<K>> zaddAll() {
        ZaddAll<K, V, KeyValue<K>> operation = new ZaddAll<>();
        operation.setKey(key());
        operation.setValues(value());
        return operation;
    }

    private SaddAll<K, V, KeyValue<K>> saddAll() {
        SaddAll<K, V, KeyValue<K>> operation = new SaddAll<>();
        operation.setKey(key());
        operation.setValues(value());
        return operation;
    }

    private RpushAll<K, V, KeyValue<K>> rpushAll() {
        RpushAll<K, V, KeyValue<K>> operation = new RpushAll<>();
        operation.setKey(key());
        operation.setValues(value());
        return operation;
    }

    private JsonSet<K, V, KeyValue<K>> jsonSet() {
        JsonSet<K, V, KeyValue<K>> operation = new JsonSet<>();
        operation.setKey(key());
        operation.setValue(value());
        return operation;
    }

    private Set<K, V, KeyValue<K>> set() {
        Set<K, V, KeyValue<K>> operation = new Set<>();
        operation.setKey(key());
        operation.setValue(value());
        return operation;
    }

    private Hset<K, V, KeyValue<K>> hset() {
        Hset<K, V, KeyValue<K>> operation = new Hset<>();
        operation.setKey(key());
        operation.setMap(value());
        return operation;
    }

    private Del<K, V, KeyValue<K>> del() {
        Del<K, V, KeyValue<K>> operation = new Del<>();
        operation.setKey(key());
        return operation;
    }

    private ExpireAt<K, V, KeyValue<K>> expire() {
        ExpireAt<K, V, KeyValue<K>> operation = new ExpireAt<>();
        operation.setKey(key());
        operation.setEpoch(KeyValue::getTtl);
        return operation;
    }

    public StructOperation<K, V> mergePolicy(MergePolicy mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    public StructOperation<K, V> streamIdPolicy(StreamIdPolicy streamIdPolicy) {
        this.streamIdPolicy = streamIdPolicy;
        return this;
    }

    private static <K> Function<KeyValue<K>, K> key() {
        return KeyValue::getKey;
    }

    @SuppressWarnings("unchecked")
    private static <K, T> Function<KeyValue<K>, T> value() {
        return kv -> (T) kv.getValue();
    }

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, KeyValue<K> item, List<RedisFuture<?>> futures) {
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
        return item == null || item.getKey() == null || (item.getValue() == null && KeyValue.hasMemoryUsage(item));
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
