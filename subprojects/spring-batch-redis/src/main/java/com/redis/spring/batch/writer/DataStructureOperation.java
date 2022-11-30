package com.redis.spring.batch.writer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	private final XaddAll<K, V, DataStructure<K>> xaddAll = XaddAll.key(this::key).messages(this::messages).build();
	private final Set<K, V, DataStructure<K>> set = Set.key(this::key).value(this::string).del(this::del).build();
	private final ZaddAll<K, V, DataStructure<K>> zadd = ZaddAll.key(this::key).members(this::zmembers).build();
	private final JsonSet<K, V, DataStructure<K>> jsonSet = JsonSet.key(this::key).value(this::string).del(this::del)
			.build();
	private final TsAddAll<K, V, DataStructure<K>> tsAddAll = TsAddAll.key(this::key).<V>samples(this::samples).build();

	public DataStructureOperation() {
	}

	public DataStructureOperation(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
		xaddAll.setArgs(xaddArgs);
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

	@SuppressWarnings("rawtypes")
	@Override
	public Collection<RedisFuture> execute(StatefulConnection<K, V> connection,
			List<? extends DataStructure<K>> items) {
		BaseRedisAsyncCommands<K, V> commands = Utils.async(connection);
		return items.stream().filter(Objects::nonNull).flatMap(s -> execute(commands, s)).collect(Collectors.toList());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Stream<RedisFuture> execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds) {
		if (ds.getValue() == null) {
			return Stream.of(del.execute(commands, ds));
		}
		Stream<RedisFuture> writes = write(commands, ds);
		if (ds.hasTtl()) {
			RedisFuture<Boolean> expire = ((RedisKeyAsyncCommands<K, V>) commands).pexpireat(ds.getKey(), ds.getTtl());
			return Stream.concat(writes, Stream.of(expire));
		}
		return writes;
	}

	@SuppressWarnings("rawtypes")
	private Stream<RedisFuture> write(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds) {
		switch (ds.getType()) {
		case HASH:
			return Stream.of(del.execute(commands, ds), hset.execute(commands, ds));
		case STRING:
			return Stream.of(set.execute(commands, ds));
		case LIST:
			return Stream.of(del.execute(commands, ds), push.execute(commands, ds));
		case SET:
			return Stream.of(del.execute(commands, ds), sadd.execute(commands, ds));
		case ZSET:
			return Stream.of(del.execute(commands, ds), zadd.execute(commands, ds));
		case STREAM:
			return Stream.concat(Stream.of(del.execute(commands, ds)), xaddAll.execute(commands, ds).stream());
		case JSON:
			return Stream.of(jsonSet.execute(commands, ds));
		case TIMESERIES:
			return Stream.concat(Stream.of(del.execute(commands, ds)), tsAddAll.execute(commands, ds).stream());
		case NONE:
			return Stream.of(del.execute(commands, ds));
		default:
			throw new IllegalArgumentException("Unknown data-structure type: " + ds.getType());
		}
	}

}
