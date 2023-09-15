package com.redis.spring.batch.reader;

import java.util.Iterator;
import java.util.List;

import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.KeyValue;

public abstract class AbstractLuaFunction<K, T extends KeyValue<K, ?>> implements LuaToKeyValueFunction<T> {

    @Override
    public T apply(List<Object> item) {
        if (item == null) {
            return null;
        }
        return keyValue(item.iterator());
    }

    @SuppressWarnings("unchecked")
    private T keyValue(Iterator<Object> iterator) {
        if (!iterator.hasNext()) {
            return null;
        }
        K key = (K) iterator.next();
        long ttl = ttl(iterator);
        DataSize memoryUsage = memoryUsage(iterator);
        T keyValue = keyValue(key, iterator);
        keyValue.setTtl(ttl);
        keyValue.setMemoryUsage(memoryUsage);
        return keyValue;
    }

    private long ttl(Iterator<Object> iterator) {
        if (iterator.hasNext()) {
            return (Long) iterator.next();
        }
        return 0;
    }

    private DataSize memoryUsage(Iterator<Object> iterator) {
        if (iterator.hasNext()) {
            return DataSize.ofBytes((Long) iterator.next());
        }
        return null;
    }

    protected abstract T keyValue(K key, Iterator<Object> iterator);

}
