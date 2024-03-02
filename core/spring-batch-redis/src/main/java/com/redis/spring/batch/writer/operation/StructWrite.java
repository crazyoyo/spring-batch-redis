package com.redis.spring.batch.writer.operation;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.Chunk;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class StructWrite<K, V> implements Operation<K, V, KeyValue<K>, Object> {

	private final Collector<KeyValue<K>, ?, Map<DataType, List<KeyValue<K>>>> groupByType = Collectors
			.groupingBy(KeyValue::getType);

	private Predicate<KeyValue<K>> existPredicate() {
		return KeyValue::exists;
	}

	@SuppressWarnings("unchecked")
	private final Predicate<KeyValue<K>> expirePredicate = Predicates.and(existPredicate(), k -> k.getTtl() > 0);

	private Predicate<KeyValue<K>> deletePredicate = Predicates.negate(existPredicate());

	private final Operation<K, V, KeyValue<K>, Object> deleteOperation = deleteOperation();

	private final Operation<K, V, KeyValue<K>, Object> expireOperation = expireOperation();

	private final Operation<K, V, KeyValue<K>, Object> hashOperation = hashOperation();

	private final Operation<K, V, KeyValue<K>, Object> jsonOperation = jsonOperation();

	private final Operation<K, V, KeyValue<K>, Object> listOperation = listOperation();

	private final Operation<K, V, KeyValue<K>, Object> setOperation = setOperation();

	private final Operation<K, V, KeyValue<K>, Object> streamOperation = streamOperation();

	private final Operation<K, V, KeyValue<K>, Object> stringOperation = stringOperation();

	private final Operation<K, V, KeyValue<K>, Object> timeseriesOperation = timeseriesOperation();

	private final Operation<K, V, KeyValue<K>, Object> zsetOperation = zsetOperation();

	private final Operation<K, V, KeyValue<K>, Object> noOperation = noOperation();

	private Noop<K, V, KeyValue<K>> noOperation() {
		return new Noop<>();
	}

	public void setOverwrite(boolean overwrite) {
		if (overwrite) {
			deletePredicate = Predicates.isTrue();
		}
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Chunk<? extends KeyValue<K>> items,
			Chunk<RedisFuture<Object>> futures) {

		Chunk<KeyValue<K>> toDelete = new Chunk<>(
				StreamSupport.stream(items.spliterator(), false).filter(deletePredicate).collect(Collectors.toList()));
		deleteOperation.execute(commands, toDelete, futures);
		Map<DataType, List<KeyValue<K>>> toWrite = StreamSupport.stream(items.spliterator(), false)
				.filter(KeyValue::exists).collect(groupByType);
		for (Entry<DataType, List<KeyValue<K>>> entry : toWrite.entrySet()) {
			operation(entry.getKey()).execute(commands, new Chunk<>(entry.getValue()), futures);
		}
		List<KeyValue<K>> toExpire = StreamSupport.stream(items.spliterator(), false).filter(expirePredicate)
				.collect(Collectors.toList());
		expireOperation.execute(commands, new Chunk<>(toExpire), futures);
	}

	private Operation<K, V, KeyValue<K>, Object> operation(DataType type) {
		switch (type) {
		case HASH:
			return hashOperation;
		case JSON:
			return jsonOperation;
		case LIST:
			return listOperation;
		case SET:
			return setOperation;
		case STREAM:
			return streamOperation;
		case STRING:
			return stringOperation;
		case TIMESERIES:
			return timeseriesOperation;
		case ZSET:
			return zsetOperation;
		default:
			return noOperation;
		}
	}

	private Hset<K, V, KeyValue<K>> hashOperation() {
		return new Hset<>(KeyValue::getKey, this::value);
	}

	private Set<K, V, KeyValue<K>> stringOperation() {
		return new Set<>(KeyValue::getKey, this::value);
	}

	private XAddArgs xaddArgs(StreamMessage<K, V> message) {
		XAddArgs args = new XAddArgs();
		if (message.getId() != null) {
			args.id(message.getId());
		}
		return args;
	}

	private XAddAll<K, V, KeyValue<K>> streamOperation() {
		XAddAll<K, V, KeyValue<K>> operation = new XAddAll<>();
		operation.setMessagesFunction(this::value);
		operation.setArgsFunction(this::xaddArgs);
		return operation;
	}

	private TsAddAll<K, V, KeyValue<K>> timeseriesOperation() {
		TsAddAll<K, V, KeyValue<K>> operation = new TsAddAll<>();
		operation.setKeyFunction(KeyValue::getKey);
		operation.setOptions(AddOptions.<K, V>builder().policy(DuplicatePolicy.LAST).build());
		operation.setSamplesFunction(this::value);
		return operation;
	}

	private ZaddAll<K, V, KeyValue<K>> zsetOperation() {
		return new ZaddAll<>(KeyValue::getKey, this::value);
	}

	private SaddAll<K, V, KeyValue<K>> setOperation() {
		return new SaddAll<>(KeyValue::getKey, this::value);
	}

	private RpushAll<K, V, KeyValue<K>> listOperation() {
		return new RpushAll<>(KeyValue::getKey, this::value);
	}

	private JsonSet<K, V, KeyValue<K>> jsonOperation() {
		return new JsonSet<>(KeyValue::getKey, this::value);
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
	private <O> O value(KeyValue<K> struct) {
		return (O) struct.getValue();
	}

}
