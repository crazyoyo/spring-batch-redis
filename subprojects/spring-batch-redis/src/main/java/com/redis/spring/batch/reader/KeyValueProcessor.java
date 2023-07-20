package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.codec.RedisCodec;

public class KeyValueProcessor<K, V> implements ItemProcessor<List<Object>, KeyValue<K>> {

    private final Function<V, String> toStringValueFunction;

    public KeyValueProcessor(RedisCodec<K, V> codec) {
        this.toStringValueFunction = Utils.toStringValueFunction(codec);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyValue<K> process(List<Object> item) throws Exception {
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
