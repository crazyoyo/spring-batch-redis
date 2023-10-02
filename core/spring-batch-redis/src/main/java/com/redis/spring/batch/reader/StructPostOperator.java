package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class StructPostOperator<K, V> implements UnaryOperator<KeyValue<K>> {

    private final Function<V, String> toStringValueFunction;

    public StructPostOperator(RedisCodec<K, V> codec) {
        this.toStringValueFunction = CodecUtils.toStringValueFunction(codec);
    }

    @Override
    public KeyValue<K> apply(KeyValue<K> t) {
        if (t != null) {
            t.setValue(value(t));
        }
        return t;
    }

    private Object value(KeyValue<K> t) {
        if (t.getValue() == null) {
            return null;
        }
        switch (t.getType()) {
            case HASH:
                return map(t);
            case SET:
                return set(t);
            case ZSET:
                return zset(t);
            case STREAM:
                return stream(t);
            case TIMESERIES:
                return timeSeries(t);
            default:
                return t.getValue();
        }
    }

    @SuppressWarnings("unchecked")
    private HashSet<V> set(KeyValue<K> t) {
        return new HashSet<>((List<V>) t.getValue());
    }

    @SuppressWarnings("unchecked")
    private List<Sample> timeSeries(KeyValue<K> t) {
        List<Sample> samples = new ArrayList<>();
        for (List<Object> sample : (List<List<Object>>) t.getValue()) {
            LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
            Long timestamp = (Long) sample.get(0);
            samples.add(Sample.of(timestamp, toDouble((V) sample.get(1))));
        }
        return samples;
    }

    private String toString(V value) {
        return toStringValueFunction.apply(value);
    }

    private double toDouble(V value) {
        return Double.parseDouble(toString(value));
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> map(KeyValue<K> t) {
        List<Object> list = (List<Object>) t.getValue();
        return map(list);
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> map(List<Object> list) {
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            map.put((K) list.get(i), (V) list.get(i + 1));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private Set<ScoredValue<V>> zset(KeyValue<K> t) {
        List<Object> list = (List<Object>) t.getValue();
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Set<ScoredValue<V>> values = new HashSet<>();
        for (int i = 0; i < list.size(); i += 2) {
            double score = toDouble((V) list.get(i + 1));
            values.add(ScoredValue.just(score, (V) list.get(i)));
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<K, V>> stream(KeyValue<K> t) {
        List<StreamMessage<K, V>> messages = new ArrayList<>();
        for (List<Object> message : (List<List<Object>>) t.getValue()) {
            LettuceAssert.isTrue(message.size() == 2, "Invalid list size: " + message.size());
            String id = toString((V) message.get(0));
            Map<K, V> body = map((List<Object>) message.get(1));
            messages.add(new StreamMessage<>(t.getKey(), id, body));
        }
        return messages;
    }

}
