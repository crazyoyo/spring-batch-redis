package com.redis.spring.batch;

import com.redis.spring.batch.writer.AbstractOperationItemWriter;
import com.redis.spring.batch.writer.BatchWriteOperation;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;
import com.redis.spring.batch.writer.WriteOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class RedisItemWriter<K, V, T> extends AbstractOperationItemWriter<K, V, T> {

    protected RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client, codec);
    }

    public static StructItemWriter<String, String> struct(AbstractRedisClient client) {
        return struct(client, StringCodec.UTF8);
    }

    public static <K, V> StructItemWriter<K, V> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new StructItemWriter<>(client, codec);
    }

    public static DumpItemWriter<String, String> dump(AbstractRedisClient client) {
        return dump(client, StringCodec.UTF8);
    }

    public static <K, V> DumpItemWriter<K, V> dump(AbstractRedisClient client, RedisCodec<K, V> codec) {
        return new DumpItemWriter<>(client, codec);
    }

    public static <T> OperationItemWriter<String, String, T> operation(AbstractRedisClient client,
            WriteOperation<String, String, T> operation) {
        return operation(client, StringCodec.UTF8, operation);
    }

    public static <K, V, T> OperationItemWriter<K, V, T> operation(AbstractRedisClient client, RedisCodec<K, V> codec,
            WriteOperation<K, V, T> operation) {
        return new OperationItemWriter<>(client, codec, operation);
    }

    public static <T> OperationItemWriter<String, String, T> operation(AbstractRedisClient client,
            BatchWriteOperation<String, String, T> operation) {
        return operation(client, StringCodec.UTF8, operation);
    }

    public static <K, V, T> OperationItemWriter<K, V, T> operation(AbstractRedisClient client, RedisCodec<K, V> codec,
            BatchWriteOperation<K, V, T> operation) {
        return new OperationItemWriter<>(client, codec, operation);
    }

}
