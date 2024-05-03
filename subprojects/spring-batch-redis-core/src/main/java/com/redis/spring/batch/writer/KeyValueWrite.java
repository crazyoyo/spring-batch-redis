package com.redis.spring.batch.writer;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;

public class KeyValueWrite<K, V> implements WriteOperation<K, V, KeyValue<K, Object>> {

	public enum WriteMode {
		MERGE, OVERWRITE
	}

	private enum OperationType {
		HSET, JSON_SET, RPUSH, SADD, XADD, SET, TS_ADD, ZADD, NONE
	}

	public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

	private final Operation<K, V, KeyValue<K, Object>, Object> noop = new Noop<>();
	private final Operation<K, V, KeyValue<K, Object>, Object> delete = new Del<>(KeyValue::getKey);
	private final Operation<K, V, KeyValue<K, Object>, Object> expire = ExpireAt.of(KeyValue::getKey, KeyValue::getTtl);
	private final Operation<K, V, KeyValue<K, Object>, Object> hset = new Hset<>(KeyValue::getKey,
			KeyValueWrite::value);
	private final Operation<K, V, KeyValue<K, Object>, Object> jsonSet = new JsonSet<>(KeyValue::getKey,
			KeyValueWrite::value);
	private final Operation<K, V, KeyValue<K, Object>, Object> rpush = new RpushAll<>(KeyValue::getKey,
			KeyValueWrite::value);
	private final Operation<K, V, KeyValue<K, Object>, Object> sadd = new SaddAll<>(KeyValue::getKey,
			KeyValueWrite::value);
	private final Operation<K, V, KeyValue<K, Object>, Object> xadd = XaddAll.of(KeyValueWrite::value,
			KeyValueWrite::xaddArgs);
	private final Operation<K, V, KeyValue<K, Object>, Object> set = new Set<>(KeyValue::getKey, KeyValueWrite::value);
	private final Operation<K, V, KeyValue<K, Object>, Object> tsAdd = TsAddAll.of(KeyValue::getKey,
			KeyValueWrite::value, AddOptions.<K, V>builder().policy(DuplicatePolicy.LAST).build());
	private final Operation<K, V, KeyValue<K, Object>, Object> zadd = new ZaddAll<>(KeyValue::getKey,
			KeyValueWrite::value);

	private WriteMode mode = DEFAULT_MODE;

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, Iterable<? extends KeyValue<K, Object>> items,
			List<RedisFuture<Object>> futures) {
		delete.execute(commands, stream(items).filter(this::shouldDelete).collect(Collectors.toList()), futures);
		stream(items).filter(KeyValue::exists).collect(Collectors.groupingBy(this::operationType))
				.forEach((k, v) -> operation(k).execute(commands, v, futures));
		expire.execute(commands, stream(items).filter(KeyValue::hasTtl).collect(Collectors.toList()), futures);
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

	private static XAddArgs xaddArgs(StreamMessage<?, ?> message) {
		XAddArgs args = new XAddArgs();
		if (message.getId() != null) {
			args.id(message.getId());
		}
		return args;
	}

	@SuppressWarnings("unchecked")
	private static <K, O> O value(KeyValue<K, Object> struct) {
		return (O) struct.getValue();
	}

	public void setMode(WriteMode mode) {
		this.mode = mode;
	}

}
