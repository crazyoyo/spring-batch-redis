package com.redis.spring.batch;

import com.redis.spring.batch.operation.Operation;
import com.redis.spring.batch.util.CodecUtils;
import com.redis.spring.batch.writer.AbstractOperationItemWriter;
import com.redis.spring.batch.writer.DumpItemWriter;
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class RedisItemWriter<K, V, T> extends AbstractOperationItemWriter<K, V, T> {

	protected RedisItemWriter(AbstractRedisClient client, RedisCodec<K, V> codec) {
		super(client, codec);
	}

	public static StructItemWriter<String, String> struct(AbstractRedisClient client) {
		return struct(client, CodecUtils.STRING_CODEC);
	}

	public static <K, V> StructItemWriter<K, V> struct(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new StructItemWriter<>(client, codec);
	}

	public static DumpItemWriter dump(AbstractRedisClient client) {
		return new DumpItemWriter(client);
	}

	public static <T> OperationItemWriter<String, String, T> operation(AbstractRedisClient client,
			Operation<String, String, T, Object> operation) {
		return operation(client, CodecUtils.STRING_CODEC, operation);
	}

	public static <K, V, T> OperationItemWriter<K, V, T> operation(AbstractRedisClient client, RedisCodec<K, V> codec,
			Operation<K, V, T, Object> operation) {
		return new OperationItemWriter<>(client, codec, operation);
	}

}
