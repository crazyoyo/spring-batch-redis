package com.redis.spring.batch.util;

import java.nio.ByteBuffer;
import java.util.function.Function;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class CodecUtils {

	public static final StringCodec STRING_CODEC = StringCodec.UTF8;

	public static final ByteArrayCodec BYTE_ARRAY_CODEC = ByteArrayCodec.INSTANCE;

	private CodecUtils() {
	}

	public static <K> Function<String, K> stringKeyFunction(RedisCodec<K, ?> codec) {
		Function<String, ByteBuffer> encode = STRING_CODEC::encodeKey;
		return encode.andThen(codec::decodeKey);
	}

	public static <K> Function<K, String> toStringKeyFunction(RedisCodec<K, ?> codec) {
		Function<K, ByteBuffer> encode = codec::encodeKey;
		return encode.andThen(STRING_CODEC::decodeKey);
	}

	public static <V> Function<String, V> stringValueFunction(RedisCodec<?, V> codec) {
		Function<String, ByteBuffer> encode = STRING_CODEC::encodeValue;
		return encode.andThen(codec::decodeValue);
	}

	public static <V> Function<V, String> toStringValueFunction(RedisCodec<?, V> codec) {
		Function<V, ByteBuffer> encode = codec::encodeValue;
		return encode.andThen(STRING_CODEC::decodeValue);
	}

	public static <K> Function<byte[], K> byteArrayKeyFunction(RedisCodec<K, ?> codec) {
		Function<byte[], ByteBuffer> encode = BYTE_ARRAY_CODEC::encodeKey;
		return encode.andThen(codec::decodeKey);
	}

	public static <K> Function<K, byte[]> toByteArrayKeyFunction(RedisCodec<K, ?> codec) {
		Function<K, ByteBuffer> encode = codec::encodeKey;
		return encode.andThen(BYTE_ARRAY_CODEC::decodeKey);
	}

	public static <V> Function<byte[], V> byteArrayValueFunction(RedisCodec<?, V> codec) {
		Function<byte[], ByteBuffer> encode = BYTE_ARRAY_CODEC::encodeValue;
		return encode.andThen(codec::decodeValue);
	}

	public static <V> Function<V, byte[]> toByteArrayValueFunction(RedisCodec<?, V> codec) {
		Function<V, ByteBuffer> encode = codec::encodeValue;
		return encode.andThen(BYTE_ARRAY_CODEC::decodeValue);
	}

}
