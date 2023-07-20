package com.redis.spring.batch.reader;

import java.util.List;
import java.util.function.Function;

import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.common.ValueType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class KeyValueReadOperation<K, V> implements Operation<K, V, K, List<Object>> {

	private static final String FILENAME = "keyvalue.lua";

	private final String digest;
	private final Function<String, V> stringValueFunction;

	private ValueType valueType = ValueType.DUMP;
	private MemoryUsageOptions memoryUsageOptions = MemoryUsageOptions.builder().build();

	public KeyValueReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.digest = Utils.loadScript(client, FILENAME);
		this.stringValueFunction = Utils.stringValueFunction(codec);
	}

	public ValueType getValueType() {
		return valueType;
	}

	public void setValueType(ValueType valueType) {
		this.valueType = valueType;
	}

	public MemoryUsageOptions getMemoryUsageOptions() {
		return memoryUsageOptions;
	}

	public void setMemoryUsageOptions(MemoryUsageOptions options) {
		this.memoryUsageOptions = options;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, K key, List<RedisFuture<List<Object>>> futures) {
		futures.add(execute(commands, key));
	}

	@SuppressWarnings("unchecked")
	public RedisFuture<List<Object>> execute(BaseRedisAsyncCommands<K, V> commands, K key) {
		RedisScriptingAsyncCommands<K, V> scripting = (RedisScriptingAsyncCommands<K, V>) commands;
		Object[] keys = { key };
		V[] args = encode(memoryUsageOptions.getLimit().toBytes(), memoryUsageOptions.getSamples(), valueType.name());
		return scripting.evalsha(digest, ScriptOutputType.MULTI, (K[]) keys, args);
	}

	@SuppressWarnings("unchecked")
	private V[] encode(Object... values) {
		Object[] encodedValues = new Object[values.length];
		for (int index = 0; index < values.length; index++) {
			encodedValues[index] = stringValueFunction.apply(String.valueOf(values[index]));
		}
		return (V[]) encodedValues;
	}

	public static Builder<String, String> builder(AbstractRedisClient client) {
		return new Builder<>(client, StringCodec.UTF8);
	}

	public static <K, V> Builder<K, V> builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
		return new Builder<>(client, codec);
	}

	public static class Builder<K, V> {

		private final AbstractRedisClient client;
		private final RedisCodec<K, V> codec;

		public Builder(AbstractRedisClient client, RedisCodec<K, V> codec) {
			this.client = client;
			this.codec = codec;
		}

		public KeyValueReadOperation<K, V> struct() {
			return build(ValueType.STRUCT);
		}

		public KeyValueReadOperation<K, V> dump() {
			return build(ValueType.DUMP);
		}

		public KeyValueReadOperation<K, V> build(ValueType valueType) {
			KeyValueReadOperation<K, V> operation = new KeyValueReadOperation<>(client, codec);
			operation.setValueType(valueType);
			return operation;
		}
	}

}
