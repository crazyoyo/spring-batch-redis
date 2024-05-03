package com.redis.spring.batch.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.common.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
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

	private static final String SCRIPT_FILENAME = "keyvalue.lua";

	private final ValueType type;
	private final RedisCodec<K, V> codec;
	private final Function<List<Object>, MemKeyValue<K, T>> function;
	private DataSize memUsageLimit = DEFAULT_MEM_USAGE_LIMIT;
	private int memUsageSamples = DEFAULT_MEM_USAGE_SAMPLES;
	private Evalsha<K, V, K> evalsha;

	public MemKeyValueRead(ValueType type, RedisCodec<K, V> codec, Function<List<Object>, MemKeyValue<K, T>> function) {
		this.type = type;
		this.codec = codec;
		this.function = function;
	}

	@Override
	public void afterPropertiesSet(StatefulRedisModulesConnection<K, V> connection) throws IOException {
		String lua = BatchUtils.readFile(SCRIPT_FILENAME);
		String digest = connection.sync().scriptLoad(lua);
		evalsha = new Evalsha<>(digest, codec, Function.identity());
		evalsha.setArgs(typeArg(), memLimitArg(), samplesArg());
	}

	private String typeArg() {
		return type.name().toLowerCase();
	}

	private long memLimitArg() {
		return memUsageLimit.toBytes();
	}

	private int samplesArg() {
		return memUsageSamples;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends K> inputs,
			List<RedisFuture<MemKeyValue<K, T>>> outputs) {
		List<RedisFuture<List<Object>>> evalOutputs = new ArrayList<>();
		evalsha.execute(commands, inputs, evalOutputs);
		evalOutputs.stream().map(f -> new MappingRedisFuture<>(f, function)).forEach(outputs::add);
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

	public static MemKeyValueRead<byte[], byte[], byte[]> dump() {
		return new MemKeyValueRead<>(ValueType.DUMP, ByteArrayCodec.INSTANCE,
				new EvalFunction<>(ByteArrayCodec.INSTANCE));
	}

	public static <K, V> MemKeyValueRead<K, V, Object> struct(RedisCodec<K, V> codec) {
		return new MemKeyValueRead<>(ValueType.STRUCT, codec, new EvalStructFunction<>(codec));
	}

	public static MemKeyValueRead<String, String, Object> struct() {
		return struct(StringCodec.UTF8);
	}

	public static MemKeyValueRead<String, String, Object> type() {
		return type(StringCodec.UTF8);
	}

	public static <K, V> MemKeyValueRead<K, V, Object> type(RedisCodec<K, V> codec) {
		return new MemKeyValueRead<>(ValueType.TYPE, codec, new EvalStructFunction<>(codec));
	}

}
