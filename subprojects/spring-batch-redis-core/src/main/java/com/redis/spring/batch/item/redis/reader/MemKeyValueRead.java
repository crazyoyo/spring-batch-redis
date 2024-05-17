package com.redis.spring.batch.item.redis.reader;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.InitializingOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class MemKeyValueRead<K, V, T> implements InitializingOperation<K, V, K, MemKeyValue<K, T>> {

	public enum ValueType {
		DUMP, STRUCT, TYPE
	}

	public static final DataSize NO_MEM_USAGE_LIMIT = DataSize.ofBytes(Long.MAX_VALUE);
	public static final DataSize DEFAULT_MEM_USAGE_LIMIT = DataSize.ofBytes(0); // No mem usage by default
	public static final int DEFAULT_MEM_USAGE_SAMPLES = 5;
	public static final ValueType DEFAULT_TYPE = ValueType.DUMP;

	private static final String SCRIPT_FILENAME = "keyvalue.lua";

	private final RedisCodec<K, V> codec;
	private final Function<List<Object>, MemKeyValue<K, T>> function;
	private final Evalsha<K, V, K> evalsha;

	private AbstractRedisClient client;
	private DataSize memUsageLimit = DEFAULT_MEM_USAGE_LIMIT;
	private int memUsageSamples = DEFAULT_MEM_USAGE_SAMPLES;
	private ValueType type = DEFAULT_TYPE;

	public MemKeyValueRead(RedisCodec<K, V> codec, Function<List<Object>, MemKeyValue<K, T>> function) {
		this.codec = codec;
		this.function = function;
		this.evalsha = new Evalsha<>(codec, Function.identity());
	}

	public void setClient(AbstractRedisClient client) {
		this.client = client;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(client, "Redis client not set");
		evalsha.setArgs(evalShaArgs());
		String lua = BatchUtils.readFile(SCRIPT_FILENAME);
		try (StatefulRedisModulesConnection<K, V> connection = RedisModulesUtils.connection(client, codec)) {
			String digest = connection.sync().scriptLoad(lua);
			evalsha.setDigest(digest);
		}
	}

	private Object[] evalShaArgs() {
		String typeArg = type.name().toLowerCase();
		long memLimitArg = memUsageLimit.toBytes();
		return new Object[] { typeArg, memLimitArg, memUsageSamples };
	}

	@Override
	public List<RedisFuture<MemKeyValue<K, T>>> execute(RedisAsyncCommands<K, V> commands,
			Iterable<? extends K> items) {
		List<RedisFuture<List<Object>>> evalOutputs = evalsha.execute(commands, items);
		return evalOutputs.stream().map(f -> new MappingRedisFuture<>(f, function)).collect(Collectors.toList());
	}

	public DataSize getMemUsageLimit() {
		return memUsageLimit;
	}

	public void setMemUsageLimit(DataSize limit) {
		this.memUsageLimit = limit;
	}

	public int getMemUsageSamples() {
		return memUsageSamples;
	}

	public void setMemUsageSamples(int samples) {
		this.memUsageSamples = samples;
	}

	public void setType(ValueType type) {
		this.type = type;
	}

	public static MemKeyValueRead<byte[], byte[], byte[]> dump() {
		return new MemKeyValueRead<>(ByteArrayCodec.INSTANCE, new EvalFunction<>(ByteArrayCodec.INSTANCE));
	}

	public static <K, V> MemKeyValueRead<K, V, Object> struct(RedisCodec<K, V> codec) {
		MemKeyValueRead<K, V, Object> operation = new MemKeyValueRead<>(codec, new EvalStructFunction<>(codec));
		operation.setType(ValueType.STRUCT);
		return operation;
	}

	public static MemKeyValueRead<String, String, Object> struct() {
		return struct(StringCodec.UTF8);
	}

	public static MemKeyValueRead<String, String, Object> type() {
		return type(StringCodec.UTF8);
	}

	public static <K, V> MemKeyValueRead<K, V, Object> type(RedisCodec<K, V> codec) {
		MemKeyValueRead<K, V, Object> operation = new MemKeyValueRead<>(codec, new EvalStructFunction<>(codec));
		operation.setType(ValueType.TYPE);
		return operation;
	}

}
