package com.redis.spring.batch;

import com.redis.spring.batch.common.Dump;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.writer.DumpWriteOperation;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructWriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public class RedisItemWriter<K, V, T extends KeyValue<K, ?>> extends OperationItemWriter<K, V, T> {

    private final Class<? extends T> type;

    public RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec, Class<? extends T> type) {
        super(client, codec, operation(type));
        this.type = type;
    }

    public Class<? extends T> getType() {
        return type;
    }

    @SuppressWarnings("unchecked")
    private static <K, V, T extends KeyValue<K, ?>> Operation<K, V, T, Object> operation(Class<? extends T> type) {
        if (Dump.class.isAssignableFrom(type)) {
            return (Operation<K, V, T, Object>) new DumpWriteOperation<>();
        }
        return (Operation<K, V, T, Object>) new StructWriteOperation<>();
    }

}
