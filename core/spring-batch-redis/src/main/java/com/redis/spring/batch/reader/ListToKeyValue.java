package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class ListToKeyValue<K, V> implements Function<List<Object>, KeyValue<K>> {

    private final Function<V, String> toStringValueFunction;

    public ListToKeyValue(RedisCodec<K, V> codec) {
        this.toStringValueFunction = CodecUtils.toStringValueFunction(codec);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyValue<K> apply(List<Object> item) {
        if (item == null) {
            return null;
        }
        KeyValue<K> keyValue = new KeyValue<>();
        Iterator<Object> iterator = item.iterator();
        if (iterator.hasNext()) {
            keyValue.setKey((K) iterator.next());
        }
        if (iterator.hasNext()) {
            keyValue.setType(toStringValueFunction.apply((V) iterator.next()));
        }
        if (iterator.hasNext()) {
            keyValue.setTtl((Long) iterator.next());
        }
        if (iterator.hasNext()) {
            keyValue.setMemoryUsage((Long) iterator.next());
        }
        if (iterator.hasNext()) {
            keyValue.setValue(iterator.next());
        }
        return keyValue;
    }

}
