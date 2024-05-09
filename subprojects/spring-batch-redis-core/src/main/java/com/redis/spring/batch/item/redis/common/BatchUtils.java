package com.redis.spring.batch.item.redis.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.util.FileCopyUtils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class BatchUtils {

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

	public static <K> Function<K, byte[]> toByteArrayKeyFunction(RedisCodec<K, ?> codec) {
		Function<K, ByteBuffer> encode = codec::encodeKey;
		return encode.andThen(ByteArrayCodec.INSTANCE::decodeKey);
	}

	@SuppressWarnings("resource")
	public static <K, V> Supplier<StatefulRedisModulesConnection<K, V>> supplier(AbstractRedisClient client,
			RedisCodec<K, V> codec, ReadFrom readFrom) {
		if (client instanceof RedisModulesClusterClient) {
			RedisModulesClusterClient clusterClient = (RedisModulesClusterClient) client;
			return () -> connection(clusterClient, codec, readFrom);
		}
		RedisModulesClient redisClient = (RedisModulesClient) client;
		return () -> redisClient.connect(codec);
	}

	public static <K, V> StatefulRedisModulesConnection<K, V> connection(AbstractRedisClient client,
			RedisCodec<K, V> codec, ReadFrom readFrom) {
		if (client instanceof RedisModulesClusterClient) {
			return connection((RedisModulesClusterClient) client, codec, readFrom);
		}
		return ((RedisModulesClient) client).connect(codec);
	}

	public static <K, V> StatefulRedisModulesClusterConnection<K, V> connection(RedisModulesClusterClient client,
			RedisCodec<K, V> codec, ReadFrom readFrom) {
		StatefulRedisModulesClusterConnection<K, V> connection = client.connect(codec);
		if (readFrom != null) {
			connection.setReadFrom(readFrom);
		}
		return connection;
	}

	public static <T> List<T> getAll(Duration timeout, Iterable<RedisFuture<T>> futures)
			throws TimeoutException, InterruptedException, ExecutionException {
		List<T> items = new ArrayList<>();
		long nanos = timeout.toNanos();
		long time = System.nanoTime();
		for (RedisFuture<T> f : futures) {
			if (timeout.isNegative()) {
				items.add(f.get());
			} else {
				if (nanos < 0) {
					throw new TimeoutException(String.format("Timed out after %s", timeout));
				}
				T item = f.get(nanos, TimeUnit.NANOSECONDS);
				items.add(item);
				long now = System.nanoTime();
				nanos -= now - time;
				time = now;
			}
		}
		return items;
	}

}
