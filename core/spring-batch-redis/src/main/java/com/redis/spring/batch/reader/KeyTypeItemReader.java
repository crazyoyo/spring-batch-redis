package com.redis.spring.batch.reader;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class KeyTypeItemReader<K, V> extends RedisItemReader<K, V, KeyType<K>> {

    public KeyTypeItemReader(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec, new KeyTypeReadOperation<>());
    }

}
