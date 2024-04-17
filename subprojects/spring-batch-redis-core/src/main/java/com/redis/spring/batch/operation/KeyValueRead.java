package com.redis.spring.batch.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.reader.EvalFunction;
import com.redis.spring.batch.reader.EvalStructFunction;
import com.redis.spring.batch.util.BatchUtils;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyValueRead<K, V> implements InitializingOperation<K, V, K, KeyValue<K>> {

	public enum ValueType {
		DUMP, STRUCT, TYPE
	}

	public static final DataSize NO_MEM_USAGE_LIMIT = DataSize.ofBytes(Long.MAX_VALUE);
	public static final DataSize DEFAULT_MEM_USAGE_LIMIT = DataSize.ofBytes(0); // No limit by default
	public static final int DEFAULT_MEM_USAGE_SAMPLES = 5;

	private static final String SCRIPT_FILENAME = "keyvalue.lua";

	private final ValueType type;
	private final RedisCodec<K, V> codec;
	private DataSize memUsageLimit = DEFAULT_MEM_USAGE_LIMIT;
	private int memUsageSamples = DEFAULT_MEM_USAGE_SAMPLES;
	private Evalsha<K, V, K> evalsha;
	private Function<List<Object>, KeyValue<K>> mappingFunction;

	public KeyValueRead(ValueType type, RedisCodec<K, V> codec) {
		this.type = type;
		this.codec = codec;
	}

	public static KeyValueRead<byte[], byte[]> dump() {
		return new KeyValueRead<>(ValueType.DUMP, ByteArrayCodec.INSTANCE);
	}

	public static <K, V> KeyValueRead<K, V> struct(RedisCodec<K, V> codec) {
		return new KeyValueRead<>(ValueType.STRUCT, codec);
	}

	public static KeyValueRead<String, String> struct() {
		return struct(StringCodec.UTF8);
	}

	public static KeyValueRead<String, String> type() {
		return new KeyValueRead<>(ValueType.TYPE, StringCodec.UTF8);
	}

	public void setMemUsageLimit(DataSize limit) {
		this.memUsageLimit = limit;
	}

	public void setMemUsageSamples(int samples) {
		this.memUsageSamples = samples;
	}

	@Override
	public void afterPropertiesSet(StatefulRedisModulesConnection<K, V> connection) throws IOException {
		String lua = BatchUtils.readFile(SCRIPT_FILENAME);
		String digest = connection.sync().scriptLoad(lua);
		evalsha = new Evalsha<>(digest, codec, Function.identity());
		evalsha.setArgs(typeArg(), memLimitArg(), samplesArg());
		mappingFunction = mappingFunction();
	}

	private Function<List<Object>, KeyValue<K>> mappingFunction() {
		switch (type) {
		case STRUCT:
		case TYPE:
			return new EvalStructFunction<>(codec);
		case DUMP:
			return new EvalFunction<>(codec);
		default:
			return new EvalFunction<>(codec);
		}
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
			List<RedisFuture<KeyValue<K>>> outputs) {
		List<RedisFuture<List<Object>>> evalOutputs = new ArrayList<>();
		evalsha.execute(commands, inputs, evalOutputs);
		evalOutputs.stream().map(f -> new MappingRedisFuture<>(f, mappingFunction)).forEach(outputs::add);
	}

}
