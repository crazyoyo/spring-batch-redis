package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataStructureType;
import com.redis.spring.batch.common.Struct;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;

public class StructReadOperation<K, V> extends AbstractKeyValueReadOperation<K, V, Struct<K>> {

    private static final String TYPE = "type";

    private final Function<V, String> toStringValueFunction;

    public StructReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec, TYPE);
        this.toStringValueFunction = CodecUtils.toStringValueFunction(codec);
    }

    @Override
    protected Struct<K> keyValue(K key, Iterator<Object> iterator) {
        // Iterator reading order must match LUA script
        Object value = iterator.hasNext() ? iterator.next() : null;
        DataStructureType type = iterator.hasNext() ? DataStructureType.of(toString(iterator.next())) : DataStructureType.NONE;
        Object object = object(type, key, value);
        return Struct.key(key).type(type).value(object).build();
    }

    @SuppressWarnings("unchecked")
    private String toString(Object value) {
        return toStringValueFunction.apply((V) value);
    }

    private Object object(DataStructureType type, K key, Object value) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case HASH:
                return map(value);
            case SET:
                return set(value);
            case ZSET:
                return zset(value);
            case STREAM:
                return stream(key, value);
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
