package com.redis.spring.batch.reader;

import java.util.Iterator;

import com.redis.spring.batch.common.Dump;

public class LuaToDumpFunction<K> extends AbstractLuaFunction<K, Dump<K>> {

    @Override
    protected Dump<K> keyValue(K key, Iterator<Object> iterator) {
        Dump<K> dump = new Dump<>();
        dump.setKey(key);
        dump.setValue((byte[]) value(iterator));
        return dump;
    }

    private Object value(Iterator<Object> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

}
