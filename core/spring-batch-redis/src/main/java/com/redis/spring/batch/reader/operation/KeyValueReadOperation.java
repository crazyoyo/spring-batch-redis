package com.redis.spring.batch.reader.operation;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.reader.MappingRedisFuture;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class KeyValueReadOperation<K, V> implements Operation<K, V, K, KeyValue<K>> {

    private final Operation<K, V, K, List<Object>> evalOperation;

    private final Function<V, String> toStringValueFunction;

    public KeyValueReadOperation(RedisCodec<K, V> codec, Operation<K, V, K, List<Object>> evalOperation) {
        this.toStringValueFunction = CodecUtils.toStringValueFunction(codec);
        this.evalOperation = evalOperation;
    }

    @Override
    public RedisFuture<KeyValue<K>> execute(BaseRedisAsyncCommands<K, V> commands, K item) {
        RedisFuture<List<Object>> future = evalOperation.execute(commands, item);
        return new MappingRedisFuture<>(future.toCompletableFuture(), this::toKeyValue);
    }

    @SuppressWarnings("unchecked")
    protected KeyValue<K> toKeyValue(List<Object> list) {
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        Iterator<Object> iterator = list.iterator();
        KeyValue<K> keyValue = new KeyValue<>();
        if (iterator.hasNext()) {
            keyValue.setKey((K) iterator.next());
        }
        if (iterator.hasNext()) {
            keyValue.setTtl((Long) iterator.next());
        }
        if (iterator.hasNext()) {
            keyValue.setMemoryUsage(DataSize.ofBytes((Long) iterator.next()));
        }
        if (iterator.hasNext()) {
            keyValue.setType(DataType.of(toString(iterator.next())));
        }
        if (iterator.hasNext()) {
            keyValue.setValue(iterator.next());
        }
        return keyValue;
    }

    @SuppressWarnings("unchecked")
    protected String toString(Object value) {
        return toStringValueFunction.apply((V) value);
    }

}
