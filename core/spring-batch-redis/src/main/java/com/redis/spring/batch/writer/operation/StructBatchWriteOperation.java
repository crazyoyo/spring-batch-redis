package com.redis.spring.batch.writer.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.SimpleBatchWriteOperation;
import com.redis.spring.batch.util.Predicates;
import com.redis.spring.batch.writer.BatchWriteOperation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class StructBatchWriteOperation<K, V> implements BatchWriteOperation<K, V, KeyValue<K>> {

    private final Collector<KeyValue<K>, ?, Map<DataType, List<KeyValue<K>>>> groupByType = Collectors
            .groupingBy(KeyValue::getType);

    private Predicate<KeyValue<K>> existPredicate() {
        return KeyValue::exists;
    }

    @SuppressWarnings("unchecked")
    private final Predicate<KeyValue<K>> expirePredicate = Predicates.and(existPredicate(), k -> k.getTtl() > 0);

    private Predicate<KeyValue<K>> deletePredicate = Predicates.negate(existPredicate());

    private final BatchWriteOperation<K, V, KeyValue<K>> deleteOperation = deleteOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> expireOperation = expireOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> hashOperation = hashOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> jsonOperation = jsonOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> listOperation = listOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> setOperation = setOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> streamOperation = streamOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> stringOperation = stringOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> timeseriesOperation = timeseriesOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> zsetOperation = zsetOperation();

    private final BatchWriteOperation<K, V, KeyValue<K>> noOperation = noOperation();

    private BatchWriteOperation<K, V, KeyValue<K>> noOperation() {
        return new SimpleBatchWriteOperation<>(new Noop<>());
    }

    public void setOverwrite(boolean overwrite) {
        if (overwrite) {
            deletePredicate = Predicates.isTrue();
        }
    }

    @Override
    public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands, Iterable<KeyValue<K>> items) {
        List<RedisFuture<Object>> futures = new ArrayList<>();
        List<KeyValue<K>> toDelete = StreamSupport.stream(items.spliterator(), false).filter(deletePredicate).collect(Collectors.toList());
        futures.addAll(deleteOperation.execute(commands, toDelete));
        Map<DataType, List<KeyValue<K>>> toWrite = StreamSupport.stream(items.spliterator(), false).filter(KeyValue::exists).collect(groupByType);
        for (Entry<DataType, List<KeyValue<K>>> entry : toWrite.entrySet()) {
            futures.addAll(operation(entry.getKey()).execute(commands, entry.getValue()));
        }
        List<KeyValue<K>> toExpire = StreamSupport.stream(items.spliterator(), false).filter(expirePredicate).collect(Collectors.toList());
        futures.addAll(expireOperation.execute(commands, toExpire));
        return futures;
    }

    private BatchWriteOperation<K, V, KeyValue<K>> operation(DataType type) {
        switch (type) {
            case HASH:
                return hashOperation;
            case JSON:
                return jsonOperation;
            case LIST:
                return listOperation;
            case SET:
                return setOperation;
            case STREAM:
                return streamOperation;
            case STRING:
                return stringOperation;
            case TIMESERIES:
                return timeseriesOperation;
            case ZSET:
                return zsetOperation;
            default:
                return noOperation;
        }
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> hashOperation() {
        Hset<K, V, KeyValue<K>> operation = new Hset<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setMapFunction(this::value);
        return new SimpleBatchWriteOperation<>(operation);
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> stringOperation() {
        Set<K, V, KeyValue<K>> operation = new Set<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setValueFunction(this::value);
        return new SimpleBatchWriteOperation<>(operation);
    }

    private XAddArgs xaddArgs(StreamMessage<K, V> message) {
        XAddArgs args = new XAddArgs();
        if (message.getId() != null) {
            args.id(message.getId());
        }
        return args;
    }

    private XAddAll<K, V, KeyValue<K>> streamOperation() {
        XAddAll<K, V, KeyValue<K>> operation = new XAddAll<>();
        operation.setMessagesFunction(this::value);
        operation.setArgsFunction(this::xaddArgs);
        return operation;
    }

    private TsAddAll<K, V, KeyValue<K>> timeseriesOperation() {
        TsAddAll<K, V, KeyValue<K>> operation = new TsAddAll<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setOptions(AddOptions.<K, V> builder().policy(DuplicatePolicy.LAST).build());
        operation.setSamplesFunction(this::value);
        return operation;
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> zsetOperation() {
        ZaddAll<K, V, KeyValue<K>> operation = new ZaddAll<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setValuesFunction(this::value);
        return new SimpleBatchWriteOperation<>(operation);
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> setOperation() {
        SaddAll<K, V, KeyValue<K>> operation = new SaddAll<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setValuesFunction(this::value);
        return new SimpleBatchWriteOperation<>(operation);
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> listOperation() {
        RpushAll<K, V, KeyValue<K>> operation = new RpushAll<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setValuesFunction(this::value);
        return new SimpleBatchWriteOperation<>(operation);
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> jsonOperation() {
        JsonSet<K, V, KeyValue<K>> operation = new JsonSet<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setValueFunction(this::value);
        return new SimpleBatchWriteOperation<>(operation);
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> deleteOperation() {
        Del<K, V, KeyValue<K>> operation = new Del<>();
        operation.setKeyFunction(KeyValue::getKey);
        return new SimpleBatchWriteOperation<>(operation);
    }

    private SimpleBatchWriteOperation<K, V, KeyValue<K>> expireOperation() {
        ExpireAt<K, V, KeyValue<K>> operation = new ExpireAt<>();
        operation.setKeyFunction(KeyValue::getKey);
        operation.setEpochFunction(KeyValue::getTtl);
        return new SimpleBatchWriteOperation<>(operation);
    }

    @SuppressWarnings("unchecked")
    private <O> O value(KeyValue<K> struct) {
        return (O) struct.getValue();
    }

}
