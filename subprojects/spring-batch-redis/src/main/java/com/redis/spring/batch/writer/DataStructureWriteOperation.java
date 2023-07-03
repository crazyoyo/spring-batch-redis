package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.AddOptions.Builder;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.Noop;
import com.redis.spring.batch.writer.operation.Restore;
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

public class DataStructureWriteOperation<K, V> implements BatchOperation<K, V, DataStructure<K>, Object> {

	public static final MergePolicy DEFAULT_MERGE_POLICY = MergePolicy.OVERWRITE;
	public static final StreamIdPolicy DEFAULT_STREAM_ID_POLICY = StreamIdPolicy.PROPAGATE;
	private static final XAddArgs EMPTY_XADD_ARGS = new XAddArgs();

	private MergePolicy mergePolicy = DEFAULT_MERGE_POLICY;
	private StreamIdPolicy streamIdPolicy = DEFAULT_STREAM_ID_POLICY;

	private final Operation<K, V, DataStructure<K>, Object> noop = new Noop<>();
	private final Hset<K, V, DataStructure<K>> hset = new Hset<>(key(), value());
	private final Set<K, V, DataStructure<K>> set = new Set<>(key(), value());
	private final JsonSet<K, V, DataStructure<K>> jsonSet = new JsonSet<>(key(), value());
	private final RpushAll<K, V, DataStructure<K>> rpush = new RpushAll<>(key(), value());
	private final SaddAll<K, V, DataStructure<K>> sadd = new SaddAll<>(key(), value());
	private final ZaddAll<K, V, DataStructure<K>> zadd = new ZaddAll<>(key(), value());
	private final TsAddAll<K, V, DataStructure<K>> tsAdd = new TsAddAll<>(key(), value(), tsAddOptions());
	private XAddAll<K, V, DataStructure<K>> xadd = xadd();

	public void setMergePolicy(MergePolicy mergePolicy) {
		this.mergePolicy = mergePolicy;
	}

	private static <K, V> AddOptions<K, V> tsAddOptions() {
		Builder<K, V> builder = AddOptions.builder();
		return builder.build();
	}

	private static <K> Function<DataStructure<K>, K> key() {
		return DataStructure::getKey;
	}

	private static <K, T> Function<DataStructure<K>, T> value() {
		return DataStructure::getValue;
	}

	public void setStreamIdPolicy(StreamIdPolicy streamIdPolicy) {
		this.streamIdPolicy = streamIdPolicy;
		this.xadd = xadd();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public List<Future<Object>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends DataStructure<K>> items) {
		List<Future<Object>> futures = new ArrayList<>();
		for (DataStructure<K> item : items) {
			if (item == null || item.getKey() == null || (item.getValue() == null && item.getMemoryUsage() > 0)) {
				continue;
			}
			if (shouldDelete(item)) {
				futures.add((Future) delete(commands, item));
			} else {
				if (mergePolicy == MergePolicy.OVERWRITE && !KeyValue.STRING.equals(item.getType())) {
					futures.add((Future) delete(commands, item));
				}
				futures.add((Future) operation(item).execute(commands, item));
				if (item.getTtl() > 0) {
					futures.add((Future) expire(commands, item));
				}
			}
		}
		return futures;
	}

	@SuppressWarnings("unchecked")
	private RedisFuture<Boolean> expire(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> item) {
		return ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(item.getKey(), item.getTtl());
	}

	private boolean shouldDelete(DataStructure<K> item) {
		return item.getValue() == null || Restore.TTL_KEY_DOES_NOT_EXIST.equals(item.getTtl()) || KeyValue.isNone(item);
	}

	@SuppressWarnings("unchecked")
	protected RedisFuture<Long> delete(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> item) {
		return ((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey());
	}

	private Operation<K, V, DataStructure<K>, ?> operation(DataStructure<K> item) {
		switch (item.getType()) {
		case KeyValue.HASH:
			return hset;
		case KeyValue.JSON:
			return jsonSet;
		case KeyValue.LIST:
			return rpush;
		case KeyValue.SET:
			return sadd;
		case KeyValue.STREAM:
			return xadd;
		case KeyValue.STRING:
			return set;
		case KeyValue.TIMESERIES:
			return tsAdd;
		case KeyValue.ZSET:
			return zadd;
		default:
			return noop;
		}
	}

	private XAddArgs xaddArgs(StreamMessage<K, V> message) {
		XAddArgs args = new XAddArgs();
		String id = message.getId();
		if (id != null) {
			args.id(id);
		}
		return args;
	}

	private XAddAll<K, V, DataStructure<K>> xadd() {
		return new XAddAll<>(DataStructure::getValue,
				streamIdPolicy == StreamIdPolicy.PROPAGATE ? this::xaddArgs : t -> EMPTY_XADD_ARGS);
	}

}
