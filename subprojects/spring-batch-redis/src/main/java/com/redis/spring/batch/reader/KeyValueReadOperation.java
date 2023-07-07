package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.common.ValueType;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.internal.LettuceAssert;

public class KeyValueReadOperation<K, V> extends ItemStreamSupport implements Operation<K, V, K, KeyValue<K>> {

	private static final String FILENAME = "keyvalue.lua";

	private final AbstractRedisClient client;
	private final RedisCodec<K, V> codec;
	private String digest;
	private ValueType valueType = ValueType.DUMP;
	private MemoryUsageOptions memoryUsageOptions = MemoryUsageOptions.builder().build();

	public KeyValueReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec) {
		this.client = client;
		this.codec = codec;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		this.digest = Utils.loadScript(Utils.connectionSupplier(client), FILENAME);
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

	/**
	 * 
	 * @param limit memory limit in bytes
	 */
	public void setMemoryUsageOptions(MemoryUsageOptions options) {
		this.memoryUsageOptions = options;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Future<KeyValue<K>> execute(BaseRedisAsyncCommands<K, V> commands, K key) {
		RedisFuture<List<Object>> result = eval((RedisScriptingAsyncCommands<K, V>) commands, key);
		return result.thenApply(this::convert).toCompletableFuture();
	}

	@SuppressWarnings("unchecked")
	private RedisFuture<List<Object>> eval(RedisScriptingAsyncCommands<K, V> commands, K key) {
		Object[] keys = { key };
		V[] args = (V[]) Stream
				.of(String.valueOf(memoryUsageOptions.getLimit().toBytes()),
						String.valueOf(memoryUsageOptions.getSamples()), valueType.name())
				.map(this::encodeValue).toArray();
		return commands.evalsha(digest, ScriptOutputType.MULTI, (K[]) keys, args);
	}

	protected String decodeValue(V value) {
		if (codec instanceof StringCodec) {
			return (String) value;
		}
		return StringCodec.UTF8.decodeValue(codec.encodeValue(value));
	}

	@SuppressWarnings("unchecked")
	protected V encodeValue(String value) {
		if (codec instanceof StringCodec) {
			return (V) value;
		}
		return codec.decodeValue(StringCodec.UTF8.encodeValue(value));
	}

	@SuppressWarnings("unchecked")
	private KeyValue<K> convert(List<Object> list) {
		if (list == null) {
			return null;
		}
		KeyValue<K> keyValue = new KeyValue<>();
		Iterator<Object> iterator = list.iterator();
		if (iterator.hasNext()) {
			keyValue.setKey((K) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setType(decodeValue((V) iterator.next()));
		}
		if (iterator.hasNext()) {
			keyValue.setTtl((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setMemoryUsage((Long) iterator.next());
		}
		if (iterator.hasNext()) {
			keyValue.setValue(value(keyValue, iterator.next()));
		}
		return keyValue;
	}

	@SuppressWarnings("unchecked")
	private Object value(KeyValue<K> keyValue, Object object) {
		if (valueType == ValueType.DUMP) {
			return object;
		}
		switch (keyValue.getType()) {
		case KeyValue.HASH:
			return map((List<Object>) object);
		case KeyValue.SET:
			return new HashSet<>((Collection<Object>) object);
		case KeyValue.ZSET:
			return zset((List<Object>) object);
		case KeyValue.STREAM:
			return stream(keyValue.getKey(), (List<Object>) object);
		case KeyValue.TIMESERIES:
			return timeSeries((List<Object>) object);
		default:
			return object;
		}
	}

	@SuppressWarnings("unchecked")
	private List<Sample> timeSeries(List<Object> list) {
		List<Sample> samples = new ArrayList<>();
		for (Object entry : list) {
			List<Object> sample = (List<Object>) entry;
			LettuceAssert.isTrue(sample.size() == 2, "Invalid list size: " + sample.size());
			samples.add(Sample.of((Long) sample.get(0), toDouble((V) sample.get(1))));
		}
		return samples;
	}

	private double toDouble(V value) {
		return Double.parseDouble(decodeValue(value));
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> map(List<Object> list) {
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Map<K, V> map = new HashMap<>();
		for (int i = 0; i < list.size(); i += 2) {
			map.put((K) list.get(i), (V) list.get(i + 1));
		}
		return map;
	}

	@SuppressWarnings("unchecked")
	private Set<ScoredValue<V>> zset(List<Object> list) {
		LettuceAssert.isTrue(list.size() % 2 == 0, "List size must be a multiple of 2");
		Set<ScoredValue<V>> values = new HashSet<>();
		for (int i = 0; i < list.size(); i += 2) {
			values.add(ScoredValue.just(toDouble((V) list.get(i + 1)), (V) list.get(i)));
		}
		return values;
	}

	@SuppressWarnings("unchecked")
	private List<StreamMessage<K, V>> stream(K key, List<Object> list) {
		List<StreamMessage<K, V>> messages = new ArrayList<>();
		for (Object object : list) {
			List<Object> entry = (List<Object>) object;
			LettuceAssert.isTrue(entry.size() == 2, "Invalid list size: " + entry.size());
			messages.add(new StreamMessage<>(key, decodeValue((V) entry.get(0)), map((List<Object>) entry.get(1))));
		}
		return messages;
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
