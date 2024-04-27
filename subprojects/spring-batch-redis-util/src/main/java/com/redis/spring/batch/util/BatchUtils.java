package com.redis.spring.batch.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

import com.hrakaroo.glob.GlobPattern;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class BatchUtils {

	private BatchUtils() {
	}

	public static String readFile(String filename) throws IOException {
		try (InputStream inputStream = BatchUtils.class.getClassLoader().getResourceAsStream(filename)) {
			return FileCopyUtils.copyToString(new InputStreamReader(inputStream));
		}
	}

	public static <K> Function<String, K> stringKeyFunction(RedisCodec<K, ?> codec) {
		Function<String, ByteBuffer> encode = StringCodec.UTF8::encodeKey;
		return encode.andThen(codec::decodeKey);
	}

	public static <K> Function<K, String> toStringKeyFunction(RedisCodec<K, ?> codec) {
		Function<K, ByteBuffer> encode = codec::encodeKey;
		return encode.andThen(StringCodec.UTF8::decodeKey);
	}

	public static <V> Function<String, V> stringValueFunction(RedisCodec<?, V> codec) {
		Function<String, ByteBuffer> encode = StringCodec.UTF8::encodeValue;
		return encode.andThen(codec::decodeValue);
	}

	public static <V> Function<V, String> toStringValueFunction(RedisCodec<?, V> codec) {
		Function<V, ByteBuffer> encode = codec::encodeValue;
		return encode.andThen(StringCodec.UTF8::decodeValue);
	}

	public static <K> Function<byte[], K> byteArrayKeyFunction(RedisCodec<K, ?> codec) {
		Function<byte[], ByteBuffer> encode = ByteArrayCodec.INSTANCE::encodeKey;
		return encode.andThen(codec::decodeKey);
	}

	public static <K> Function<K, byte[]> toByteArrayKeyFunction(RedisCodec<K, ?> codec) {
		Function<K, ByteBuffer> encode = codec::encodeKey;
		return encode.andThen(ByteArrayCodec.INSTANCE::decodeKey);
	}

	public static <V> Function<byte[], V> byteArrayValueFunction(RedisCodec<?, V> codec) {
		Function<byte[], ByteBuffer> encode = ByteArrayCodec.INSTANCE::encodeValue;
		return encode.andThen(codec::decodeValue);
	}

	public static <V> Function<V, byte[]> toByteArrayValueFunction(RedisCodec<?, V> codec) {
		Function<V, ByteBuffer> encode = codec::encodeValue;
		return encode.andThen(ByteArrayCodec.INSTANCE::decodeValue);
	}

	public static Supplier<StatefulRedisModulesConnection<String, String>> supplier(AbstractRedisClient client) {
		return supplier(client, StringCodec.UTF8);
	}

	public static Supplier<StatefulRedisModulesConnection<String, String>> supplier(AbstractRedisClient client,
			ReadFrom readFrom) {
		return supplier(client, StringCodec.UTF8, readFrom);
	}

	public static <K, V> Supplier<StatefulRedisModulesConnection<K, V>> supplier(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return supplier(client, codec, null);
	}

	public static <K, V> Supplier<StatefulRedisModulesConnection<K, V>> supplier(AbstractRedisClient client,
			RedisCodec<K, V> codec, ReadFrom readFrom) {
		if (client instanceof RedisModulesClusterClient) {
			return () -> connection((RedisModulesClusterClient) client, codec, readFrom);
		}
		return () -> connection((RedisModulesClient) client, codec);
	}

	public static <K, V> StatefulRedisModulesConnection<K, V> connection(AbstractRedisClient client,
			RedisCodec<K, V> codec, ReadFrom readFrom) {
		if (client instanceof RedisModulesClusterClient) {
			return connection((RedisModulesClusterClient) client, codec, readFrom);
		}
		return connection((RedisModulesClient) client, codec);
	}

	public static StatefulRedisModulesConnection<String, String> connection(AbstractRedisClient client) {
		return connection(client, StringCodec.UTF8, null);
	}

	public static <K, V> StatefulRedisModulesConnection<K, V> connection(RedisModulesClient client,
			RedisCodec<K, V> codec) {
		return client.connect(codec);
	}

	public static <K, V> StatefulRedisModulesConnection<K, V> connection(RedisModulesClusterClient client,
			RedisCodec<K, V> codec, ReadFrom readFrom) {
		StatefulRedisModulesClusterConnection<K, V> connection = client.connect(codec);
		if (readFrom != null) {
			connection.setReadFrom(readFrom);
		}
		return connection;
	}

	public static Predicate<String> globPredicate(String match) {
		if (!StringUtils.hasLength(match)) {
			return s -> true;
		}
		return GlobPattern.compile(match)::matches;
	}
}
