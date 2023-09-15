package com.redis.spring.batch.writer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.common.Struct.Type;
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

    private final Map<Type, Operation<K, V, Struct<K>, Object>> operations = operations();

    private Map<Type, Operation<K, V, Struct<K>, Object>> operations() {
        Map<Type, Operation<K, V, Struct<K>, Object>> map = new HashMap<>();
        map.put(Type.NONE, new Noop<>());
        map.put(Type.HASH, hashOperation());
        map.put(Type.STRING, stringOperation());
        map.put(Type.JSON, jsonOperation());
        map.put(Type.LIST, listOperation());
        map.put(Type.SET, setOperation());
        map.put(Type.ZSET, zsetOperation());
        map.put(Type.TIMESERIES, timeseriesOperation());
        map.put(Type.STREAM, streamOperation());
        return map;
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
        operations.get(item.getType()).execute(commands, item, futures);
        if (item.getTtl() > 0) {
            expire.execute(commands, item, futures);
        }
    }

    private Operation<K, V, Struct<K>, Object> hashOperation() {
        Hset<K, V, Struct<K>> operation = new Hset<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setMap(this::value);
        return operation;
    }

    private Operation<K, V, Struct<K>, Object> stringOperation() {
        Set<K, V, Struct<K>> operation = new Set<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValue(this::value);
        return operation;
    }

    private static boolean exists(Struct<?> item) {
        return item.getValue() != null && item.getTtl() != Restore.TTL_KEY_DOES_NOT_EXIST && item.getType() != Type.NONE;
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
        operation.setMessages(this::value);
        operation.setArgs(this::xaddArgs);
        return operation;
    }

    private TsAddAll<K, V, Struct<K>> timeseriesOperation() {
        TsAddAll<K, V, Struct<K>> operation = new TsAddAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setSamples(this::value);
        return operation;
    }

    private ZaddAll<K, V, Struct<K>> zsetOperation() {
        ZaddAll<K, V, Struct<K>> operation = new ZaddAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValues(this::value);
        return operation;
    }

    private SaddAll<K, V, Struct<K>> setOperation() {
        SaddAll<K, V, Struct<K>> operation = new SaddAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValues(this::value);
        return operation;
    }

    private RpushAll<K, V, Struct<K>> listOperation() {
        RpushAll<K, V, Struct<K>> operation = new RpushAll<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValues(this::value);
        return operation;
    }

    private JsonSet<K, V, Struct<K>> jsonOperation() {
        JsonSet<K, V, Struct<K>> operation = new JsonSet<>();
        operation.setKeyFunction(Struct::getKey);
        operation.setValue(this::value);
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
        operation.setEpoch(Struct::getTtl);
        return operation;
    }

    @SuppressWarnings("unchecked")
    private <T> T value(Struct<K> struct) {
        return (T) struct.getValue();
    }

}
