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
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class ListToKeyValueStruct<K, V> implements UnaryOperator<KeyValue<K>> {

    private final Function<V, String> toStringValueFunction;

    public ListToKeyValueStruct(RedisCodec<K, V> codec) {
        this.toStringValueFunction = CodecUtils.toStringValueFunction(codec);
    }

    @Override
    public KeyValue<K> apply(KeyValue<K> item) {
        if (item == null || item.getValue() == null) {
            return item;
        }
        switch (item.getType()) {
            case KeyValue.HASH:
                item.setValue(map(item.getValue()));
                break;
            case KeyValue.SET:
                item.setValue(new HashSet<>(item.getValue()));
                break;
            case KeyValue.ZSET:
                item.setValue(zset(item.getValue()));
                break;
            case KeyValue.STREAM:
                item.setValue(stream(item.getKey(), item.getValue()));
                break;
            case KeyValue.TIMESERIES:
                item.setValue(timeSeries(item.getValue()));
                break;
            default:
                // do nothing
                break;
        }
        return item;
    }

    @SuppressWarnings("unchecked")
    private List<Sample> timeSeries(List<Object> list) {
        List<Sample> samples = new ArrayList<>();
        for (Object entry : list) {
            List<Object> sample = (List<Object>) entry;
            LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
            Long timestamp = (Long) sample.get(0);
            double value = toDouble((V) sample.get(1));
            samples.add(Sample.of(timestamp, value));
        }
        return samples;
    }

    private double toDouble(V value) {
        return Double.parseDouble(toStringValueFunction.apply(value));
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> map(List<Object> list) {
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            K key = (K) list.get(i);
            V value = (V) list.get(i + 1);
            map.put(key, value);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private Set<ScoredValue<V>> zset(List<Object> list) {
        LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
        Set<ScoredValue<V>> values = new HashSet<>();
        for (int i = 0; i < list.size(); i += 2) {
            double score = toDouble((V) list.get(i + 1));
            V value = (V) list.get(i);
            values.add(ScoredValue.just(score, value));
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private List<StreamMessage<K, V>> stream(K key, List<Object> list) {
        List<StreamMessage<K, V>> messages = new ArrayList<>();
        for (Object object : list) {
            List<Object> entry = (List<Object>) object;
            LettuceAssert.isTrue(entry.size() == 2, "Invalid list size: " + entry.size());
            String id = toStringValueFunction.apply((V) entry.get(0));
            Map<K, V> body = map((List<Object>) entry.get(1));
            messages.add(new StreamMessage<>(key, id, body));
        }
        return messages;
    }

}
