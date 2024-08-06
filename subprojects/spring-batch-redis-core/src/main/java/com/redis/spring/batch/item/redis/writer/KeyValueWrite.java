package com.redis.spring.batch.item.redis.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.common.Operation;
import com.redis.spring.batch.item.redis.writer.impl.ClassifierOperation;
import com.redis.spring.batch.item.redis.writer.impl.Del;
import com.redis.spring.batch.item.redis.writer.impl.ExpireAt;
import com.redis.spring.batch.item.redis.writer.impl.Hset;
import com.redis.spring.batch.item.redis.writer.impl.JsonSet;
import com.redis.spring.batch.item.redis.writer.impl.Rpush;
import com.redis.spring.batch.item.redis.writer.impl.Sadd;
import com.redis.spring.batch.item.redis.writer.impl.Set;
import com.redis.spring.batch.item.redis.writer.impl.TsAdd;
import com.redis.spring.batch.item.redis.writer.impl.Xadd;
import com.redis.spring.batch.item.redis.writer.impl.Zadd;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class KeyValueWrite<K, V> implements Operation<K, V, KeyValue<K, Object>, Object> {

	public enum WriteMode {
		MERGE, OVERWRITE
	}

	public static final WriteMode DEFAULT_MODE = WriteMode.OVERWRITE;

	private final Del<K, V, KeyValue<K, Object>> delete = delete();
	private final ExpireAt<K, V, KeyValue<K, Object>> expire = expire();
	private final ClassifierOperation<K, V, KeyValue<K, Object>, DataType> write = writeOperation();

	private WriteMode mode = DEFAULT_MODE;

	@Override
	public List<RedisFuture<Object>> execute(RedisAsyncCommands<K, V> commands,
			List<? extends KeyValue<K, Object>> items) {
		List<RedisFuture<Object>> futures = new ArrayList<>();
		futures.addAll(delete.execute(commands, toDelete(items)));
		futures.addAll(write.execute(commands, toWrite(items)));
		futures.addAll(expire.execute(commands, toExpire(items)));
		return futures;
	}

	private List<? extends KeyValue<K, Object>> toWrite(List<? extends KeyValue<K, Object>> items) {
		return stream(items).filter(KeyValue::exists).collect(Collectors.toList());
	}

	private ClassifierOperation<K, V, KeyValue<K, Object>, DataType> writeOperation() {
		ClassifierOperation<K, V, KeyValue<K, Object>, DataType> operation = new ClassifierOperation<>(KeyValue::type);
		operation.setOperation(DataType.HASH, new Hset<>(KeyValue::getKey, KeyValueWrite::value));
		operation.setOperation(DataType.JSON, new JsonSet<>(KeyValue::getKey, KeyValueWrite::value));
		operation.setOperation(DataType.STRING, new Set<>(KeyValue::getKey, KeyValueWrite::value));
		operation.setOperation(DataType.LIST, new Rpush<>(KeyValue::getKey, KeyValueWrite::value));
		operation.setOperation(DataType.SET, new Sadd<>(KeyValue::getKey, KeyValueWrite::value));
		operation.setOperation(DataType.STREAM, new Xadd<>(KeyValue::getKey, KeyValueWrite::value));
		TsAdd<K, V, KeyValue<K, Object>> tsAdd = new TsAdd<>(KeyValue::getKey, KeyValueWrite::value);
		tsAdd.setOptions(AddOptions.<K, V>builder().policy(DuplicatePolicy.LAST).build());
		operation.setOperation(DataType.TIMESERIES, tsAdd);
		operation.setOperation(DataType.ZSET, new Zadd<>(KeyValue::getKey, KeyValueWrite::value));
		return operation;
	}

	private Del<K, V, KeyValue<K, Object>> delete() {
		return new Del<>(KeyValue::getKey);
	}

	private ExpireAt<K, V, KeyValue<K, Object>> expire() {
		ExpireAt<K, V, KeyValue<K, Object>> operation = new ExpireAt<>(KeyValue::getKey);
		operation.setTimestampFunction(KeyValue::getTtl);
		return operation;
	}

	private List<KeyValue<K, Object>> toExpire(Iterable<? extends KeyValue<K, Object>> items) {
		return stream(items).filter(KeyValue::hasTtl).collect(Collectors.toList());
	}

	private List<KeyValue<K, Object>> toDelete(Iterable<? extends KeyValue<K, Object>> items) {
		return stream(items).filter(this::shouldDelete).collect(Collectors.toList());
	}

	private Stream<? extends KeyValue<K, Object>> stream(Iterable<? extends KeyValue<K, Object>> items) {
		return StreamSupport.stream(items.spliterator(), false);
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

	public Del<K, V, KeyValue<K, Object>> getDelete() {
		return delete;
	}

	public ExpireAt<K, V, KeyValue<K, Object>> getExpire() {
		return expire;
	}

	public ClassifierOperation<K, V, KeyValue<K, Object>, DataType> getWrite() {
		return write;
	}

	public WriteMode getMode() {
		return mode;
	}

	public static <K, V> KeyValueWrite<K, V> create(WriteMode mode) {
		KeyValueWrite<K, V> operation = new KeyValueWrite<>();
		operation.setMode(mode);
		return operation;
	}

}
