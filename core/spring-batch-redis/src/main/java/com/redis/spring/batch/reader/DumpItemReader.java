package com.redis.spring.batch.reader;

import com.redis.spring.batch.common.Dump;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class DumpItemReader<K, V> extends KeyValueItemReader<K, V, Dump<K>> {

    public DumpItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec, new DumpReadOperation<>(client, codec));
    }

}
