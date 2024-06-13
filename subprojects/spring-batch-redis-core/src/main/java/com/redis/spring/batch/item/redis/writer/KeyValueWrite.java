package com.redis.spring.batch.item.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Operation;
import com.redis.spring.batch.item.redis.writer.operation.Del;
import com.redis.spring.batch.item.redis.writer.operation.ExpireAt;
import com.redis.spring.batch.item.redis.writer.operation.Hset;
import com.redis.spring.batch.item.redis.writer.operation.JsonSet;
import com.redis.spring.batch.item.redis.writer.operation.Noop;
import com.redis.spring.batch.item.redis.writer.operation.Rpush;
import com.redis.spring.batch.item.redis.writer.operation.Sadd;
import com.redis.spring.batch.item.redis.writer.operation.Set;
import com.redis.spring.batch.item.redis.writer.operation.TsAdd;
import com.redis.spring.batch.item.redis.writer.operation.Xadd;
import com.redis.spring.batch.item.redis.writer.operation.Zadd;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class KeyValueWrite<K, V> implements Operation<K, V, KeyValue<K, Object>, Object> {

	public enum WriteMode {
		MERGE, OVERWRITE
	}

	private enum OperationType {
		HSET, JSON_SET, RPUSH, SADD, XADD, SET, TS_ADD, ZADD, NONE
	}

	public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

	private final Noop<K, V, KeyValue<K, Object>> noop = new Noop<>();
	private final Del<K, V, KeyValue<K, Object>> delete = delete();
	private final ExpireAt<K, V, KeyValue<K, Object>> expire = expire();
	private final Hset<K, V, KeyValue<K, Object>> hset = hset();
	private final JsonSet<K, V, KeyValue<K, Object>> jsonSet = jsonSet();
	private final Rpush<K, V, KeyValue<K, Object>> rpush = rpush();
	private final Sadd<K, V, KeyValue<K, Object>> sadd = sadd();
	private final Xadd<K, V, KeyValue<K, Object>> xadd = xadd();
	private final Set<K, V, KeyValue<K, Object>> set = set();
	private final TsAdd<K, V, KeyValue<K, Object>> tsAdd = tsAdd();
	private final Zadd<K, V, KeyValue<K, Object>> zadd = zadd();

	private WriteMode mode = DEFAULT_MODE;

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands,
			Iterable<? extends KeyValue<K, Object>> items) {
		List<RedisFuture<Object>> futures = new ArrayList<>();
		futures.addAll(delete.execute(commands, toDelete(items)));
		groupByOperation(items).forEach((k, v) -> futures.addAll(operation(k).execute(commands, v)));
		futures.addAll(expire.execute(commands, toExpire(items)));
		return futures;
	}

	private Zadd<K, V, KeyValue<K, Object>> zadd() {
		return new Zadd<>(KeyValue::getKey, KeyValueWrite::value);
	}

	private TsAdd<K, V, KeyValue<K, Object>> tsAdd() {
		TsAdd<K, V, KeyValue<K, Object>> operation = new TsAdd<>(KeyValue::getKey, KeyValueWrite::value);
		operation.setOptions(AddOptions.<K, V>builder().policy(DuplicatePolicy.LAST).build());
		return operation;
	}

	private Del<K, V, KeyValue<K, Object>> delete() {
		return new Del<>(KeyValue::getKey);
	}

	private ExpireAt<K, V, KeyValue<K, Object>> expire() {
		ExpireAt<K, V, KeyValue<K, Object>> operation = new ExpireAt<>(KeyValue::getKey);
		operation.setTimestampFunction(KeyValue::absoluteTTL);
		return operation;
	}

	private Xadd<K, V, KeyValue<K, Object>> xadd() {
		return new Xadd<>(KeyValue::getKey, KeyValueWrite::value);
	}

	private Rpush<K, V, KeyValue<K, Object>> rpush() {
		return new Rpush<>(KeyValue::getKey, KeyValueWrite::value);
	}

	private Sadd<K, V, KeyValue<K, Object>> sadd() {
		return new Sadd<>(KeyValue::getKey, KeyValueWrite::value);
	}

	private Set<K, V, KeyValue<K, Object>> set() {
		return new Set<>(KeyValue::getKey, KeyValueWrite::value);
	}

	private JsonSet<K, V, KeyValue<K, Object>> jsonSet() {
		return new JsonSet<>(KeyValue::getKey, KeyValueWrite::value);
	}

	private Hset<K, V, KeyValue<K, Object>> hset() {
		return new Hset<>(KeyValue::getKey, KeyValueWrite::value);
	}

	private List<KeyValue<K, Object>> toExpire(Iterable<? extends KeyValue<K, Object>> items) {
		return stream(items).filter(KeyValue::hasTtl).collect(Collectors.toList());
	}

	private List<KeyValue<K, Object>> toDelete(Iterable<? extends KeyValue<K, Object>> items) {
		return stream(items).filter(this::shouldDelete).collect(Collectors.toList());
	}

	private Map<OperationType, List<KeyValue<K, Object>>> groupByOperation(
			Iterable<? extends KeyValue<K, Object>> items) {
		return stream(items).filter(KeyValue::exists).collect(Collectors.groupingBy(this::operationType));
	}

	private Stream<? extends KeyValue<K, Object>> stream(Iterable<? extends KeyValue<K, Object>> items) {
		return StreamSupport.stream(items.spliterator(), false);
	}

	private OperationType operationType(KeyValue<K, Object> item) {
		DataType type = KeyValue.type(item);
		if (type == null) {
			return OperationType.NONE;
		}
		switch (type) {
		case HASH:
			return OperationType.HSET;
		case JSON:
			return OperationType.JSON_SET;
		case LIST:
			return OperationType.RPUSH;
		case SET:
			return OperationType.SADD;
		case STREAM:
			return OperationType.XADD;
		case STRING:
			return OperationType.SET;
		case TIMESERIES:
			return OperationType.TS_ADD;
		case ZSET:
			return OperationType.ZADD;
		default:
			return OperationType.NONE;
		}
	}

	private Operation<K, V, KeyValue<K, Object>, Object> operation(OperationType operationType) {
		switch (operationType) {
		case HSET:
			return hset;
		case JSON_SET:
			return jsonSet;
		case RPUSH:
			return rpush;
		case SADD:
			return sadd;
		case XADD:
			return xadd;
		case SET:
			return set;
		case TS_ADD:
			return tsAdd;
		case ZADD:
			return zadd;
		default:
			return noop;
		}
	}

	private boolean shouldDelete(KeyValue<K, Object> item) {
		return mode == WriteMode.OVERWRITE || !KeyValue.exists(item);
	}

	@SuppressWarnings("unchecked")
	private static <K, O> O value(KeyValue<K, Object> struct) {
		return (O) struct.getValue();
	}

	public void setMode(WriteMode mode) {
		this.mode = mode;
	}

	public static <K, V> KeyValueWrite<K, V> create(WriteMode mode) {
		KeyValueWrite<K, V> operation = new KeyValueWrite<>();
		operation.setMode(mode);
		return operation;
	}

}
