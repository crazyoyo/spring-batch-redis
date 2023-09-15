package com.redis.spring.batch.reader;

import java.util.Iterator;

import com.redis.spring.batch.common.Dump;

public class LuaToDumpFunction<K> extends AbstractLuaFunction<K, Dump<K>> {

    private static final String VALUE_TYPE = "dump";

    @Override
    protected Dump<K> keyValue(K key, Iterator<Object> iterator) {
        return new Dump<>(key, (byte[]) value(iterator));
    }

    private Object value(Iterator<Object> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public String getValueType() {
        return VALUE_TYPE;
    }

}
