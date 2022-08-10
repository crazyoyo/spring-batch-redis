package com.redis.spring.batch.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.DataStructure;
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
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class DataStructureOperation<K, V> implements PipelinedOperation<K, V, DataStructure<K>> {

	private final Log log = LogFactory.getLog(getClass());

	private UnknownTypePolicy unknownTypePolicy = UnknownTypePolicy.LOG;
	private final Del<K, V, DataStructure<K>> del = Del.of(this::key);
	private final Hset<K, V, DataStructure<K>> hset = Hset.key(this::key).map(this::map).build();
	private final RpushAll<K, V, DataStructure<K>> push = RpushAll.key(this::key).members(this::members).build();
	private final SaddAll<K, V, DataStructure<K>> sadd = SaddAll.key(this::key).members(this::members).build();
	private final XaddAll<K, V, DataStructure<K>> xadd = XaddAll.key(this::key).messages(this::messages).build();
	private final Set<K, V, DataStructure<K>> set = Set.key(this::key).value(this::string).del(this::del).build();
	private final ZaddAll<K, V, DataStructure<K>> zadd = ZaddAll.key(this::key).members(this::zmembers).build();
	private final JsonSet<K, V, DataStructure<K>> jsonSet = JsonSet.key(this::key).path(this::jsonPath)
			.value(this::string).del(this::del).build();
	private final TsAddAll<K, V, DataStructure<K>> tsAdd = TsAddAll.key(this::key).<V>samples(this::samples).build();
	private final RedisCodec<K, V> codec;

	public enum UnknownTypePolicy {
		IGNORE, FAIL, LOG
	}

	public DataStructureOperation(RedisCodec<K, V> codec) {
		this.codec = codec;
	}

	public XaddAll<K, V, DataStructure<K>> getXadd() {
		return xadd;
	}

	@SuppressWarnings("unchecked")
	private Map<K, V> map(DataStructure<K> ds) {
		Map<K, V> map = (Map<K, V>) ds.getValue();
		if (map.isEmpty()) {
			return null;
		}
		return map;
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
		Collection<ScoredValue<V>> members = (Collection<ScoredValue<V>>) ds.getValue();
		if (members.isEmpty()) {
			return null;
		}
		return members;
	}

	@SuppressWarnings("unchecked")
	private Collection<V> members(DataStructure<K> ds) {
		Collection<V> members = (Collection<V>) ds.getValue();
		if (members.isEmpty()) {
			return null;
		}
		return members;
	}

	@SuppressWarnings("unchecked")
	private V string(DataStructure<K> ds) {
		return (V) ds.getValue();
	}

	private K jsonPath(DataStructure<K> ds) {
		return codec.decodeKey(StringCodec.UTF8.encodeKey("$"));
	}

	private boolean del(DataStructure<K> ds) {
		return ds.getValue() == null;
	}

	private K key(DataStructure<K> ds) {
		return ds.getKey();
	}

	public void setUnknownTypePolicy(UnknownTypePolicy unknownTypePolicy) {
		Assert.notNull(unknownTypePolicy, "Unknown-type policy must not be null");
		this.unknownTypePolicy = unknownTypePolicy;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<RedisFuture<?>> execute(BaseRedisAsyncCommands<K, V> commands,
			List<? extends DataStructure<K>> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (DataStructure<K> ds : items) {
			if (ds == null) {
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
			case UNKNOWN:
			case NONE:
				handleUnknownType(ds);
				break;
			}
			if (ds.hasTtl()) {
				futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(ds.getKey(), ds.getTtl()));
			}
		}
		return futures;
	}

	private void handleUnknownType(DataStructure<K> ds) {
		switch (unknownTypePolicy) {
		case FAIL:
			throw new IllegalArgumentException(String.format("Unknown type %s for key %s", ds.getType(), ds.getKey()));
		case LOG:
			log.warn(String.format("Unknown type %s for key %s", ds.getType(), string(ds.getKey())));
			break;
		case IGNORE:
			break;
		}

	}

	private String string(K key) {
		return StringCodec.UTF8.decodeKey(codec.encodeKey(key));
	}

}
