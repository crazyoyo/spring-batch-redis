package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.List;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.spring.batch.common.BatchOperation;
import com.redis.spring.batch.common.DataStructure;
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
	private final Hset<K, V, DataStructure<K>> hset = new Hset<>(DataStructure::getKey, DataStructure::getValue);

	private final Set<K, V, DataStructure<K>> set = new Set<>(DataStructure::getKey, DataStructure::getValue);

	private final JsonSet<K, V, DataStructure<K>> jsonSet = new JsonSet<>(DataStructure::getKey,
			DataStructure::getValue);

	private final RpushAll<K, V, DataStructure<K>> rpush = new RpushAll<>(DataStructure::getKey,
			DataStructure::getValue);

	private final SaddAll<K, V, DataStructure<K>> sadd = new SaddAll<>(DataStructure::getKey, DataStructure::getValue);

	private final ZaddAll<K, V, DataStructure<K>> zadd = new ZaddAll<>(DataStructure::getKey, DataStructure::getValue);

	private final TsAddAll<K, V, DataStructure<K>> tsAdd = new TsAddAll<>(DataStructure::getKey,
			DataStructure::getValue, AddOptions.<K, V>builder().build());

	private XAddAll<K, V, DataStructure<K>> xadd = xadd();

	public void setMergePolicy(MergePolicy mergePolicy) {
		this.mergePolicy = mergePolicy;
	}

	public void setStreamIdPolicy(StreamIdPolicy streamIdPolicy) {
		this.streamIdPolicy = streamIdPolicy;
		this.xadd = xadd();
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
				if (mergePolicy == MergePolicy.OVERWRITE && !DataStructure.STRING.equals(item.getType())) {
					futures.add(delete(commands, item));
				}
				futures.add((RedisFuture) operation(item).execute(commands, item));
				if (item.getTtl() != null && item.getTtl() > 0) {
					futures.add((RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(item.getKey(),
							item.getTtl()));
				}
			}
		}
		return futures;
	}

	private boolean shouldDelete(DataStructure<K> item) {
		return item.getValue() == null || Restore.TTL_KEY_DOES_NOT_EXIST.equals(item.getTtl())
				|| DataStructure.isNone(item);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected RedisFuture<Object> delete(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> item) {
		return (RedisFuture) ((RedisKeyAsyncCommands<K, V>) commands).del(item.getKey());
	}

	private Operation<K, V, DataStructure<K>, ?> operation(DataStructure<K> item) {
		switch (item.getType()) {
		case DataStructure.HASH:
			return hset;
		case DataStructure.JSON:
			return jsonSet;
		case DataStructure.LIST:
			return rpush;
		case DataStructure.SET:
			return sadd;
		case DataStructure.STREAM:
			return xadd;
		case DataStructure.STRING:
			return set;
		case DataStructure.TIMESERIES:
			return tsAdd;
		case DataStructure.ZSET:
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
