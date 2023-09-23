package com.redis.spring.batch.writer;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.redis.spring.batch.common.DataStructureType;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.Struct;
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

public class StructWriteOperation<K, V> implements Operation<K, V, Struct<K>, Object> {

    private final ExpireAt<K, V, Struct<K>> expire = expireOperation();

    private final Del<K, V, Struct<K>> del = delOperation();

    private final Map<DataStructureType, Operation<K, V, Struct<K>, Object>> operations = operations();

    private boolean merge;

    private Map<DataStructureType, Operation<K, V, Struct<K>, Object>> operations() {
        EnumMap<DataStructureType, Operation<K, V, Struct<K>, Object>> map = new EnumMap<>(DataStructureType.class);
        map.put(DataStructureType.NONE, new Noop<>());
        map.put(DataStructureType.HASH, hashOperation());
        map.put(DataStructureType.STRING, stringOperation());
        map.put(DataStructureType.JSON, jsonOperation());
        map.put(DataStructureType.LIST, listOperation());
        map.put(DataStructureType.SET, setOperation());
        map.put(DataStructureType.ZSET, zsetOperation());
        map.put(DataStructureType.TIMESERIES, timeseriesOperation());
        map.put(DataStructureType.STREAM, streamOperation());
        return map;
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    @Override
    public void execute(BaseRedisAsyncCommands<K, V> commands, Struct<K> item, List<RedisFuture<Object>> futures) {
        if (exists(item)) {
            write(commands, item, futures);
        } else {
            delete(commands, item, futures);
        }
    }

    protected void delete(BaseRedisAsyncCommands<K, V> commands, Struct<K> item, List<RedisFuture<Object>> futures) {
        del.execute(commands, item, futures);
    }

    protected void write(BaseRedisAsyncCommands<K, V> commands, Struct<K> item, List<RedisFuture<Object>> futures) {
        if (!merge && item.getType() != DataStructureType.STRING) {
            delete(commands, item, futures);
        }
        operations.get(item.getType()).execute(commands, item, futures);
        if (item.getTtl() > 0) {
            expire.execute(commands, item, futures);
        }
    }

    private Operation<K, V, Struct<K>, Object> hashOperation() {
        Hset<K, V, Struct<K>> operation = new Hset<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setMapFunction(this::value);
        return operation;
    }

    private Operation<K, V, Struct<K>, Object> stringOperation() {
        Set<K, V, Struct<K>> operation = new Set<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValueFunction(this::value);
        return operation;
    }

    private static boolean exists(Struct<?> item) {
        return item.getValue() != null && item.getTtl() != Restore.TTL_KEY_DOES_NOT_EXIST && item.getType() != DataStructureType.NONE;
    }

    private XAddArgs xaddArgs(StreamMessage<K, V> message) {
        XAddArgs args = new XAddArgs();
        if (message.getId() != null) {
            args.id(message.getId());
        }
        return args;
    }

    private XAddAll<K, V, Struct<K>> streamOperation() {
        XAddAll<K, V, Struct<K>> operation = new XAddAll<>();
        operation.setMessagesFunction(this::value);
        operation.setArgsFunction(this::xaddArgs);
        return operation;
    }

    private TsAddAll<K, V, Struct<K>> timeseriesOperation() {
        TsAddAll<K, V, Struct<K>> operation = new TsAddAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setSamplesFunction(this::value);
        return operation;
    }

    private ZaddAll<K, V, Struct<K>> zsetOperation() {
        ZaddAll<K, V, Struct<K>> operation = new ZaddAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValuesFunction(this::value);
        return operation;
    }

    private SaddAll<K, V, Struct<K>> setOperation() {
        SaddAll<K, V, Struct<K>> operation = new SaddAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValuesFunction(this::value);
        return operation;
    }

    private RpushAll<K, V, Struct<K>> listOperation() {
        RpushAll<K, V, Struct<K>> operation = new RpushAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValuesFunction(this::value);
        return operation;
    }

    private JsonSet<K, V, Struct<K>> jsonOperation() {
        JsonSet<K, V, Struct<K>> operation = new JsonSet<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValueFunction(this::value);
        return operation;
    }

    private Del<K, V, Struct<K>> delOperation() {
        Del<K, V, Struct<K>> operation = new Del<>();
        operation.setKeyFunction(Struct::getKey);
        return operation;
    }

    private ExpireAt<K, V, Struct<K>> expireOperation() {
        ExpireAt<K, V, Struct<K>> operation = new ExpireAt<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setEpochFunction(Struct::getTtl);
        return operation;
    }

    @SuppressWarnings("unchecked")
    private <T> T value(Struct<K> struct) {
        return (T) struct.getValue();
    }

}
