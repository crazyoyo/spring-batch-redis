package com.redis.spring.batch.operation;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class KeyValueWrite<K, V> implements Operation<K, V, KeyValue<K>, Object> {

	public enum WriteMode {
		MERGE, OVERWRITE
	}

	public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

	private final Operation<K, V, KeyValue<K>, Object> deleteOperation = deleteOperation();
	private final Operation<K, V, KeyValue<K>, Object> expireOperation = expireOperation();
	private final Operation<K, V, KeyValue<K>, Object> noOperation = new Noop<>();
	private final Map<Type, Operation<K, V, KeyValue<K>, Object>> typeOperations = typeOperations();

	private Predicate<KeyValue<K>> deletePredicate = deletePredicate(DEFAULT_MODE);

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends KeyValue<K>> items,
			List<RedisFuture<Object>> futures) {
		List<? extends KeyValue<K>> toDelete = StreamSupport.stream(items.spliterator(), false).filter(deletePredicate)
				.collect(Collectors.toList());
		deleteOperation.execute(commands, toDelete, futures);
		Map<Type, List<KeyValue<K>>> toWrite = StreamSupport.stream(items.spliterator(), false).filter(KeyValue::exists)
				.collect(Collectors.groupingBy(KeyValue::getType));
		for (Entry<Type, List<KeyValue<K>>> entry : toWrite.entrySet()) {
			typeOperations.getOrDefault(entry.getKey(), noOperation).execute(commands, entry.getValue(), futures);
		}
		List<KeyValue<K>> toExpire = StreamSupport.stream(items.spliterator(), false).filter(k -> k.getTtl() > 0)
				.collect(Collectors.toList());
		expireOperation.execute(commands, toExpire, futures);
	}

	private static <K, V> Map<Type, Operation<K, V, KeyValue<K>, Object>> typeOperations() {
		Map<Type, Operation<K, V, KeyValue<K>, Object>> operations = new EnumMap<>(Type.class);
		operations.put(Type.HASH, new Hset<>(KeyValue::getKey, KeyValueWrite::value));
		operations.put(Type.JSON, new JsonSet<>(KeyValue::getKey, KeyValueWrite::value));
		operations.put(Type.LIST, new RpushAll<>(KeyValue::getKey, KeyValueWrite::value));
		operations.put(Type.SET, new SaddAll<>(KeyValue::getKey, KeyValueWrite::value));
		XaddAll<K, V, KeyValue<K>> streamOperation = new XaddAll<>(KeyValueWrite::value);
		streamOperation.setArgsFunction(KeyValueWrite::xaddArgs);
		operations.put(Type.STREAM, streamOperation);
		operations.put(Type.STRING, new Set<>(KeyValue::getKey, KeyValueWrite::value));
		TsAddAll<K, V, KeyValue<K>> timeseriesOperation = new TsAddAll<>();
		timeseriesOperation.setKeyFunction(KeyValue::getKey);
		timeseriesOperation.setOptions(AddOptions.<K, V>builder().policy(DuplicatePolicy.LAST).build());
		timeseriesOperation.setSamplesFunction(KeyValueWrite::value);
		operations.put(Type.TIMESERIES, timeseriesOperation);
		operations.put(Type.ZSET, new ZaddAll<>(KeyValue::getKey, KeyValueWrite::value));
		return operations;
	}

	private static XAddArgs xaddArgs(StreamMessage<?, ?> message) {
		XAddArgs args = new XAddArgs();
		if (message.getId() != null) {
			args.id(message.getId());
		}
		return args;
	}

	private Operation<K, V, KeyValue<K>, Object> deleteOperation() {
		return new Del<>(KeyValue::getKey);
	}

	private ExpireAt<K, V, KeyValue<K>> expireOperation() {
		ExpireAt<K, V, KeyValue<K>> operation = new ExpireAt<>(KeyValue::getKey);
		operation.setEpochFunction(KeyValue::getTtl);
		return operation;
	}

	@SuppressWarnings("unchecked")
	private static <K, O> O value(KeyValue<K> struct) {
		return (O) struct.getValue();
	}

	public void setMode(WriteMode mode) {
		deletePredicate = deletePredicate(mode);
	}

	private static <K> Predicate<KeyValue<K>> deletePredicate(WriteMode mode) {
		if (mode == WriteMode.OVERWRITE) {
			return Predicates.isTrue();
		}
		return Predicate.not(KeyValue::exists);
	}

}
