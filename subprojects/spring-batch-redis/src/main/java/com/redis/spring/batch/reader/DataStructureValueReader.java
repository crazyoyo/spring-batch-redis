package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.NoOpRedisFuture;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.common.DataStructure.Type;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class DataStructureValueReader<K, V> extends AbstractValueReader<K, V, DataStructure<K>> {

	private static final TimeRange TIME_RANGE = TimeRange.unbounded();
	private static final RangeOptions RANGE_OPTIONS = RangeOptions.builder().build();
	private static final Range<String> XRANGE = Range.create("-", "+");

	public DataStructureValueReader(GenericObjectPool<StatefulConnection<K, V>> pool) {
		super(pool);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected List<DataStructure<K>> read(StatefulConnection<K, V> connection, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		List<RedisFuture<String>> typeFutures = new ArrayList<>(keys.size());
		for (K key : keys) {
			typeFutures.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
		}
		connection.flushCommands();
		List<DataStructure<K>> dataStructures = new ArrayList<>(keys.size());
		List<RedisFuture<Long>> ttlFutures = new ArrayList<>(keys.size());
		List<RedisFuture<?>> valueFutures = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			K key = keys.get(index);
			String type = typeFutures.get(index).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			valueFutures.add(value(commands, key, type));
			ttlFutures.add(absoluteTTL(commands, key));
			DataStructure<K> dataStructure = new DataStructure<>();
			dataStructure.setKey(key);
			dataStructure.setTypeString(type);
			dataStructures.add(dataStructure);
		}
		connection.flushCommands();
		for (int index = 0; index < dataStructures.size(); index++) {
			DataStructure<K> dataStructure = dataStructures.get(index);
			RedisFuture<?> valueFuture = valueFutures.get(index);
			if (valueFuture != null) {
				Object value = valueFuture.get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
				if (value != null) {
					setValue(dataStructure, value);
				}
			}
			long absoluteTTL = ttlFutures.get(index).get(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
			dataStructure.setTtl(absoluteTTL);
		}
		return dataStructures;
	}

	/**
	 * Sets the value of a data structure
	 * 
	 * @param dataStructure the data structure to set the value of
	 * @param value         data structure value, not null
	 */
	protected void setValue(DataStructure<K> dataStructure, Object value) {
		dataStructure.setValue(value);
	}

	@SuppressWarnings("unchecked")
	private RedisFuture<?> value(BaseRedisAsyncCommands<K, V> commands, K key, String typeName) {
		Type type = Type.of(typeName);
		if (type == null) {
			return NoOpRedisFuture.NO_OP_REDIS_FUTURE;
		}
		switch (type) {
		case HASH:
			return ((RedisHashAsyncCommands<K, V>) commands).hgetall(key);
		case LIST:
			return ((RedisListAsyncCommands<K, V>) commands).lrange(key, 0, -1);
		case SET:
			return ((RedisSetAsyncCommands<K, V>) commands).smembers(key);
		case STREAM:
			return ((RedisStreamAsyncCommands<K, V>) commands).xrange(key, XRANGE);
		case STRING:
			return ((RedisStringAsyncCommands<K, V>) commands).get(key);
		case ZSET:
			return ((RedisSortedSetAsyncCommands<K, V>) commands).zrangeWithScores(key, 0, -1);
		case JSON:
			return ((RedisJSONAsyncCommands<K, V>) commands).jsonGet(key);
		case TIMESERIES:
			return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsRange(key, TIME_RANGE, RANGE_OPTIONS);
		default:
			return NoOpRedisFuture.NO_OP_REDIS_FUTURE;
		}
	}

}
