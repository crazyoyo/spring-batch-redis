package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.spring.batch.common.AsyncOperation;
import com.redis.spring.batch.common.BatchAsyncOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.writer.DataStructureWriteOptions.MergePolicy;
import com.redis.spring.batch.writer.DataStructureWriteOptions.StreamIdPolicy;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Noop;
import com.redis.spring.batch.writer.operation.RpushAll;
import com.redis.spring.batch.writer.operation.SaddAll;
import com.redis.spring.batch.writer.operation.Set;
import com.redis.spring.batch.writer.operation.TsAddAll;
import com.redis.spring.batch.writer.operation.XAddAll;
import com.redis.spring.batch.writer.operation.ZaddAll;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class DataStructureWriteOperation<K, V> implements BatchAsyncOperation<K, V, DataStructure<K>, Object> {

	private static final XAddArgs EMPTY_XADD_ARGS = new XAddArgs();

	private final boolean deleteFirst;
	private final AsyncOperation<K, V, DataStructure<K>, Object> noop = new Noop<>();
	private Map<String, AsyncOperation<K, V, DataStructure<K>, ?>> operations;

	public DataStructureWriteOperation(DataStructureWriteOptions options) {
		this.deleteFirst = options.getMergePolicy() == MergePolicy.OVERWRITE;
		this.operations = new HashMap<>();
		operations.put(DataStructure.HASH, hset());
		operations.put(DataStructure.STRING, set());
		operations.put(DataStructure.JSON, jsonSet());
		operations.put(DataStructure.LIST, rpush());
		operations.put(DataStructure.SET, sadd());
		operations.put(DataStructure.ZSET, zadd());
		operations.put(DataStructure.TIMESERIES, tsAdd());
		operations.put(DataStructure.STREAM, xadd(xaddArgs(options.getStreamIdPolicy())));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<RedisFuture<Object>> execute(BaseRedisAsyncCommands<K, V> commands,
			List<? extends DataStructure<K>> items) {
		List<RedisFuture<Object>> futures = new ArrayList<>();
		for (DataStructure<K> item : items) {
			if (item == null || item.getKey() == null) {
				continue;
			}
			if (shouldDelete(item)) {
				futures.add(delete(commands, item));
			} else {
				if (deleteFirst && !DataStructure.STRING.equals(item.getType())) {
					futures.add(delete(commands, item));
				}
				futures.add((RedisFuture) operation(item).execute(commands, item));
				if (item.hasTtl()) {
					futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(item.getKey(),
							item.getTtl()));
				}
			}
		}
		return futures;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected RedisFuture<Object> delete(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> item) {
		return (RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey());
	}

	protected boolean shouldDelete(DataStructure<K> item) {
		return item.isInexistent() || item.getValue() == null || DataStructure.isNone(item);
	}

	private AsyncOperation<K, V, DataStructure<K>, ?> operation(DataStructure<K> item) {
		return operations.getOrDefault(item.getType(), noop);
	}

	private Function<StreamMessage<K, V>, XAddArgs> xaddArgs(StreamIdPolicy streamIdPolicy) {
		if (streamIdPolicy == StreamIdPolicy.PROPAGATE) {
			return this::xaddArgs;
		}
		return t -> EMPTY_XADD_ARGS;
	}

	private XAddArgs xaddArgs(StreamMessage<K, V> message) {
		XAddArgs args = new XAddArgs();
		String id = message.getId();
		if (id != null) {
			args.id(id);
		}
		return args;
	}

	private Hset<K, V, DataStructure<K>> hset() {
		return new Hset<>(DataStructure::getKey, DataStructure::getValue);
	}

	private Set<K, V, DataStructure<K>> set() {
		return new Set<>(DataStructure::getKey, DataStructure::getValue);
	}

	private JsonSet<K, V, DataStructure<K>> jsonSet() {
		return new JsonSet<>(DataStructure::getKey, DataStructure::getValue);
	}

	private RpushAll<K, V, DataStructure<K>> rpush() {
		return new RpushAll<>(DataStructure::getKey, DataStructure::getValue);
	}

	private SaddAll<K, V, DataStructure<K>> sadd() {
		return new SaddAll<>(DataStructure::getKey, DataStructure::getValue);
	}

	private ZaddAll<K, V, DataStructure<K>> zadd() {
		return new ZaddAll<>(DataStructure::getKey, DataStructure::getValue);
	}

	private TsAddAll<K, V, DataStructure<K>> tsAdd() {
		return new TsAddAll<>(DataStructure::getKey, DataStructure::getValue, AddOptions.<K, V>builder().build());
	}

	private XAddAll<K, V, DataStructure<K>> xadd(Function<StreamMessage<K, V>, XAddArgs> args) {
		return new XAddAll<>(DataStructure::getValue, args);
	}

}
