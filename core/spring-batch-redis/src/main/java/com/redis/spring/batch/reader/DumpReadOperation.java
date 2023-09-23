package com.redis.spring.batch.reader;

import java.util.Iterator;

import com.redis.spring.batch.common.Dump;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class DumpReadOperation<K, V> extends AbstractKeyValueReadOperation<K, V, Dump<K>> {

    private static final String TYPE = "dump";

    public DumpReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec, TYPE);
    }

    @Override
    protected Dump<K> keyValue(K key, Iterator<Object> iterator) {
        byte[] value = iterator.hasNext() ? (byte[]) iterator.next() : null;
        return Dump.key(key).value(value).build();
    }

}
