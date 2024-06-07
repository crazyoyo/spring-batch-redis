package com.redis.spring.batch.item.redis.reader;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.InitializingOperation;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;

public class KeyValueRead<K, V, T> implements InitializingOperation<K, V, K, KeyValue<K, T>> {

	protected enum ValueType {
		DUMP, STRUCT, NONE
	}

	public static final DataSize MEM_USAGE_OFF = DataSize.ofBytes(-1);
	public static final DataSize MEM_USAGE_NO_LIMIT = DataSize.ofBytes(0);
	public static final DataSize DEFAULT_MEM_USAGE_LIMIT = DataSize.ofMegabytes(10);
	public static final int DEFAULT_MEM_USAGE_SAMPLES = 5;
	public static final String ATTR_MEMORY_USAGE = "memory-usage";

	private static final String SCRIPT_FILENAME = "keyvalue.lua";

	private final RedisCodec<K, V> codec;
	private final Evalsha<K, V, K> evalsha;
	private final Function<V, String> toStringValueFunction;
	private final ValueType mode;

	private AbstractRedisClient client;
	private DataSize memUsageLimit = DEFAULT_MEM_USAGE_LIMIT;
	private int memUsageSamples = DEFAULT_MEM_USAGE_SAMPLES;

	protected KeyValueRead(ValueType mode, RedisCodec<K, V> codec) {
		this.mode = mode;
		this.codec = codec;
		this.evalsha = new Evalsha<>(codec, Function.identity());
		this.toStringValueFunction = BatchUtils.toStringValueFunction(codec);
	}

	public void setClient(AbstractRedisClient client) {
		this.client = client;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(client, "Redis client not set");
		evalsha.setArgs(mode, memUsageLimit.toBytes(), memUsageSamples);
		String lua = BatchUtils.readFile(SCRIPT_FILENAME);
		try (StatefulRedisModulesConnection<K, V> connection = RedisModulesUtils.connection(client, codec)) {
			String digest = connection.sync().scriptLoad(lua);
			evalsha.setDigest(digest);
		}
	}

	@Override
	public List<RedisFuture<KeyValue<K, T>>> execute(RedisAsyncCommands<K, V> commands, Iterable<? extends K> items) {
		List<RedisFuture<List<Object>>> evalOutputs = evalsha.execute(commands, items);
		return evalOutputs.stream().map(f -> new MappingRedisFuture<>(f, this::convert)).collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	public KeyValue<K, T> convert(List<Object> list) {
		Iterator<Object> iterator = list.iterator();
		KeyValue<K, T> keyValue = new KeyValue<>();
		keyValue.setKey((K) iterator.next());
		keyValue.setTime((Long) iterator.next());
		keyValue.setTtl((Long) iterator.next());
		if (iterator.hasNext()) {
			keyValue.setMemoryUsage((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setType(toString(iterator.next()));
		}
		if (iterator.hasNext()) {
			keyValue.setValue((T) iterator.next());
		}
		return keyValue;
	}

	@SuppressWarnings("unchecked")
	protected String toString(Object value) {
		return toStringValueFunction.apply((V) value);
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

	public static <K, V> KeyValueRead<K, V, byte[]> dump(RedisCodec<K, V> codec) {
		return new KeyValueRead<>(ValueType.DUMP, codec);
	}

	public static <K, V> KeyValueRead<K, V, Object> type(RedisCodec<K, V> codec) {
		return new KeyValueRead<>(ValueType.NONE, codec);
	}

	public static <K, V> KeyValueRead<K, V, Object> struct(RedisCodec<K, V> codec) {
		return new KeyValueStructRead<>(codec);
	}

}
