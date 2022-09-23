package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.writer.operation.Del;
import com.redis.spring.batch.writer.operation.Hset;
import com.redis.spring.batch.writer.operation.JsonSet;
import com.redis.spring.batch.writer.operation.RpushAll;
import com.redis.spring.batch.writer.operation.SaddAll;
import com.redis.spring.batch.writer.operation.Set;
import com.redis.spring.batch.writer.operation.TsAddAll;
import com.redis.spring.batch.writer.operation.XaddAll;
import com.redis.spring.batch.writer.operation.ZaddAll;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class DataStructureOperation<K, V> implements PipelinedOperation<K, V, DataStructure<K>> {

	private final Del<K, V, DataStructure<K>> del = Del.of(this::key);
	private final Hset<K, V, DataStructure<K>> hset = Hset.key(this::key).map(this::map).build();
	private final RpushAll<K, V, DataStructure<K>> push = RpushAll.key(this::key).members(this::members).build();
	private final SaddAll<K, V, DataStructure<K>> sadd = SaddAll.key(this::key).members(this::members).build();
	private final XaddAll<K, V, DataStructure<K>> xadd = XaddAll.key(this::key).messages(this::messages).build();
	private final Set<K, V, DataStructure<K>> set = Set.key(this::key).value(this::string).del(this::del).build();
	private final ZaddAll<K, V, DataStructure<K>> zadd = ZaddAll.key(this::key).members(this::zmembers).build();
	private final JsonSet<K, V, DataStructure<K>> jsonSet = JsonSet.key(this::key).value(this::string).del(this::del)
			.build();
	private final TsAddAll<K, V, DataStructure<K>> tsAdd = TsAddAll.key(this::key).<V>samples(this::samples).build();

	public DataStructureOperation() {
	}

	public DataStructureOperation(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
		xadd.setArgs(xaddArgs);
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> map(DataStructure<K> ds) {
		return (Map<K, V>) ds.getValue();
	}

	@SuppressWarnings("unchecked")
	private Collection<StreamMessage<K, V>> messages(DataStructure<K> ds) {
		return (Collection<StreamMessage<K, V>>) ds.getValue();
	}

	@SuppressWarnings("unchecked")
	private Collection<Sample> samples(DataStructure<K> ds) {
		return (Collection<Sample>) ds.getValue();
	}

	@SuppressWarnings("unchecked")
	private Collection<ScoredValue<V>> zmembers(DataStructure<K> ds) {
		return (Collection<ScoredValue<V>>) ds.getValue();
	}

	@SuppressWarnings("unchecked")
	private Collection<V> members(DataStructure<K> ds) {
		return (Collection<V>) ds.getValue();
	}

	@SuppressWarnings("unchecked")
	private V string(DataStructure<K> ds) {
		return (V) ds.getValue();
	}

	private boolean del(DataStructure<K> ds) {
		return ds.getValue() == null;
	}

	private K key(DataStructure<K> ds) {
		return ds.getKey();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<RedisFuture<?>> execute(StatefulConnection<K, V> connection,
			List<? extends DataStructure<K>> items) {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (DataStructure<K> ds : items) {
			if (ds == null) {
				continue;
			}
			if (ds.getValue() == null) {
				futures.add(del.execute(commands, ds));
				continue;
			}
			switch (ds.getType()) {
			case HASH:
				futures.add(del.execute(commands, ds));
				futures.add(hset.execute(commands, ds));
				break;
			case STRING:
				futures.add(set.execute(commands, ds));
				break;
			case LIST:
				futures.add(del.execute(commands, ds));
				futures.add(push.execute(commands, ds));
				break;
			case SET:
				futures.add(del.execute(commands, ds));
				futures.add(sadd.execute(commands, ds));
				break;
			case ZSET:
				futures.add(del.execute(commands, ds));
				futures.add(zadd.execute(commands, ds));
				break;
			case STREAM:
				futures.add(del.execute(commands, ds));
				futures.addAll(xadd.execute(commands, ds));
				break;
			case JSON:
				futures.add(jsonSet.execute(commands, ds));
				break;
			case TIMESERIES:
				futures.add(del.execute(commands, ds));
				futures.addAll(tsAdd.execute(commands, ds));
				break;
			case NONE:
				futures.add(del.execute(commands, ds));
				break;
			case UNKNOWN:
				throw new UnknownTypeException(ds);
			}
			if (ds.hasTtl()) {
				futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(ds.getKey(), ds.getTtl()));
			}
		}
		futures.removeIf(Objects::isNull);
		return futures;
	}

	public static class UnknownTypeException extends RuntimeException {

		private static final long serialVersionUID = 1L;
		private final DataStructure<?> dataStructure;

		public UnknownTypeException(DataStructure<?> dataStructure) {
			this.dataStructure = dataStructure;
		}

		public DataStructure<?> getDataStructure() {
			return dataStructure;
		}

	}

}
