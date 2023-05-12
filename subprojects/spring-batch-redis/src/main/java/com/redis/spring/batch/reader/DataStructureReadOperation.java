package com.redis.spring.batch.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.NoOpRedisFuture;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.AbstractRedisClient;
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
import io.lettuce.core.codec.RedisCodec;

public class DataStructureReadOperation<K, V> extends AbstractReadOperation<K, V, DataStructure<K>> {

	private static final Log log = LogFactory.getLog(DataStructureReadOperation.class);

	private static final TimeRange TIME_RANGE = TimeRange.unbounded();
	private static final RangeOptions RANGE_OPTIONS = RangeOptions.builder().build();
	private static final Range<String> XRANGE = Range.create("-", "+");

	public DataStructureReadOperation(AbstractRedisClient client, RedisCodec<K, V> codec, PoolOptions poolOptions) {
		super(client, codec, poolOptions);
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<DataStructure<K>> read(StatefulConnection<K, V> connection, List<? extends K> keys)
			throws InterruptedException, ExecutionException, TimeoutException {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		List<RedisFuture<String>> types = new ArrayList<>(keys.size());
		for (K key : keys) {
			types.add(((RedisKeyAsyncCommands<K, V>) commands).type(key));
		}
		connection.flushCommands();
		List<DataStructure<K>> items = new ArrayList<>(keys.size());
		List<RedisFuture<Long>> ttls = new ArrayList<>(keys.size());
		List<RedisFuture<?>> values = new ArrayList<>(keys.size());
		for (int index = 0; index < keys.size(); index++) {
			K key = keys.get(index);
			String type = get(types.get(index), connection);
			DataStructure<K> item = DataStructure.of(key, type);
			items.add(item);
			values.add(value(commands, item));
			ttls.add(absoluteTTL(commands, key));
		}
		connection.flushCommands();
		for (int index = 0; index < items.size(); index++) {
			DataStructure<K> item = items.get(index);
			try {
				Object value = get(values.get(index), connection);
				item.setValue(value);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			} catch (Exception e) {
				log.debug(String.format("Could not get value for key %s", item.getKey()), e);
			}
			try {
				Long absoluteTTL = get(ttls.get(index), connection);
				item.setTtl(absoluteTTL);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			} catch (Exception e) {
				log.debug(String.format("Could not get TTL for key %s", item.getKey()), e);
			}

		}
		return items;
	}

	@SuppressWarnings("unchecked")
	private RedisFuture<?> value(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds) {
		switch (ds.getType()) {
		case DataStructure.HASH:
			return ((RedisHashAsyncCommands<K, V>) commands).hgetall(ds.getKey());
		case DataStructure.LIST:
			return ((RedisListAsyncCommands<K, V>) commands).lrange(ds.getKey(), 0, -1);
		case DataStructure.SET:
			return ((RedisSetAsyncCommands<K, V>) commands).smembers(ds.getKey());
		case DataStructure.STREAM:
			return ((RedisStreamAsyncCommands<K, V>) commands).xrange(ds.getKey(), XRANGE);
		case DataStructure.STRING:
			return ((RedisStringAsyncCommands<K, V>) commands).get(ds.getKey());
		case DataStructure.ZSET:
			return ((RedisSortedSetAsyncCommands<K, V>) commands).zrangeWithScores(ds.getKey(), 0, -1);
		case DataStructure.JSON:
			return ((RedisJSONAsyncCommands<K, V>) commands).jsonGet(ds.getKey());
		case DataStructure.TIMESERIES:
			return ((RedisTimeSeriesAsyncCommands<K, V>) commands).tsRange(ds.getKey(), TIME_RANGE, RANGE_OPTIONS);
		default:
			return NoOpRedisFuture.NO_OP_REDIS_FUTURE;
		}
	}

}
