package com.redis.spring.batch.reader.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class StructReadOperation<K, V> extends KeyValueReadOperation<K, V> {

    public StructReadOperation(RedisCodec<K, V> codec, Operation<K, V, K, List<Object>> evalOperation) {
        super(codec, evalOperation);
    }

    @Override
    protected KeyValue<K> toKeyValue(List<Object> list) {
        KeyValue<K> keyValue = super.toKeyValue(list);
        keyValue.setValue(object(keyValue));
        return keyValue;
    }

    private Object object(KeyValue<K> keyValue) {
        if (keyValue.getValue() == null) {
            return null;
        }
        Object value = keyValue.getValue();
        switch (keyValue.getType()) {
            case HASH:
                return map(value);
            case SET:
                return set(value);
            case ZSET:
                return zset(value);
            case STREAM:
                return stream(keyValue.getKey(), value);
            case TIMESERIES:
                return timeSeries(value);
            default:
                return value;
        }
    }

    @SuppressWarnings("unchecked")
    private HashSet<V> set(Object value) {
        return new HashSet<>((List<V>) value);
    }

    @SuppressWarnings("unchecked")
    private List<Sample> timeSeries(Object value) {
        List<Sample> samples = new ArrayList<>();
        for (Object entry : (List<Object>) value) {
            List<Object> sample = (List<Object>) entry;
            LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
            Long timestamp = (Long) sample.get(0);
            samples.add(Sample.of(timestamp, toDouble(sample.get(1))));
        }
        return samples;
    }

    private double toDouble(Object value) {
        return Double.parseDouble(toString(value));
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> map(Object value) {
        List<Object> list = (List<Object>) value;
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            map.put((K) list.get(i), (V) list.get(i + 1));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private Set<ScoredValue<V>> zset(Object value) {
        List<Object> list = (List<Object>) value;
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Set<ScoredValue<V>> values = new HashSet<>();
        for (int i = 0; i < list.size(); i += 2) {
            double score = toDouble(list.get(i + 1));
            values.add(ScoredValue.just(score, (V) list.get(i)));
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<K, V>> stream(K key, Object value) {
        List<StreamMessage<K, V>> messages = new ArrayList<>();
        for (Object object : (List<Object>) value) {
            List<Object> list = (List<Object>) object;
            LettuceAssert.isTrue(list.size() == 2, "Invalid list size: " + list.size());
            String id = toString(list.get(0));
            Map<K, V> body = map(list.get(1));
            messages.add(new StreamMessage<>(key, id, body));
        }
        return messages;
    }

}
