package com.redis.spring.batch.writer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisJSONAsyncCommands;
import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class DataStructureOperationExecutor<K, V> implements OperationExecutor<K, V, DataStructure<K>> {

	private static final Logger log = LoggerFactory.getLogger(DataStructureOperationExecutor.class);

	private Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;
	private int chunkSize = RedisItemReader.DEFAULT_CHUNK_SIZE;
	private UnknownTypePolicy unknownTypePolicy = UnknownTypePolicy.LOG;
	private final HashOperation hashOperation = new HashOperation();
	private final ListOperation listOperation = new ListOperation();
	private final SetOperation setOperation = new SetOperation();
	private final JsonOperation jsonOperation;
	private final StreamOperation streamOperation = new StreamOperation();
	private final StringOperation stringOperation = new StringOperation();
	private final ZsetOperation zsetOperation = new ZsetOperation();
	private final TimeSeriesOperation timeseriesOperation = new TimeSeriesOperation();
	private final RedisCodec<K, V> codec;

	public enum UnknownTypePolicy {
		IGNORE, FAIL, LOG
	}

	public DataStructureOperationExecutor(RedisCodec<K, V> codec) {
		this.codec = codec;
		this.jsonOperation = new JsonOperation(codec.decodeKey(StringCodec.UTF8.encodeKey("$")));
	}

	public void setTimeout(Duration timeout) {
		Utils.assertPositive(timeout, "Timeout duration");
		this.timeout = timeout;
	}

	public void setChunkSize(int chunkSize) {
		Utils.assertPositive(chunkSize, "Chunk size");
		this.chunkSize = chunkSize;
	}

	public void setUnknownTypePolicy(UnknownTypePolicy unknownTypePolicy) {
		Assert.notNull(unknownTypePolicy, "Unknown-type policy must not be null");
		this.unknownTypePolicy = unknownTypePolicy;
	}

	@Override
	public void execute(BaseRedisAsyncCommands<K, V> commands, List<? extends DataStructure<K>> items,
			List<Future<?>> futures) {
		for (DataStructure<K> item : items) {
			execute(commands, item, futures);
		}
	}

	@SuppressWarnings("unchecked")
	private void execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
		if (ds == null) {
			return;
		}
		if (ds.getValue() == null) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			return;
		}
		Type type;
		try {
			type = Type.of(ds.getType());
		} catch (Exception e) {
			switch (unknownTypePolicy) {
			case FAIL:
				throw new IllegalArgumentException(
						String.format("Unknown type %s for key %s", ds.getType(), ds.getKey()));
			case LOG:
				log.warn("Unknown type {} for key {}", ds.getType(), string(ds.getKey()));
				break;
			case IGNORE:
				break;
			}
			return;
		}
		switch (type) {
		case HASH:
			hashOperation.execute(commands, ds, futures);
			break;
		case STRING:
			stringOperation.execute(commands, ds, futures);
			break;
		case LIST:
			listOperation.execute(commands, ds, futures);
			break;
		case SET:
			setOperation.execute(commands, ds, futures);
			break;
		case ZSET:
			zsetOperation.execute(commands, ds, futures);
			break;
		case STREAM:
			streamOperation.execute(commands, ds, futures);
			break;
		case JSON:
			jsonOperation.execute(commands, ds, futures);
			break;
		case TIMESERIES:
			timeseriesOperation.execute(commands, ds, futures);
			break;
		case NONE:
			// do nothing
			break;
		}
		if (ds.hasTtl()) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(ds.getKey(), ds.getTtl()));
		}
	}

	private String string(K key) {
		return StringCodec.UTF8.decodeKey(codec.encodeKey(key));
	}

	public interface DataStructureOperation<K, V> {

		void execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures);

	}

	protected abstract static class DelOperation<K, V> implements DataStructureOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public void execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			doExecute(commands, ds, futures);
		}

		protected abstract void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds,
				List<Future<?>> futures);

	}

	public class HashOperation extends DelOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			Map<K, V> map = (Map<K, V>) ds.getValue();
			if (!map.isEmpty()) {
				futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(ds.getKey(), map));
			}
		}
	}

	public class ListOperation extends DelOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			Collection<V> list = (Collection<V>) ds.getValue();
			if (!list.isEmpty()) {
				futures.add(((RedisListAsyncCommands<K, V>) commands).rpush(ds.getKey(), (V[]) list.toArray()));
			}
		}
	}

	public class SetOperation extends DelOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			Collection<V> set = (Collection<V>) ds.getValue();
			if (!set.isEmpty()) {
				futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(ds.getKey(), (V[]) set.toArray()));
			}

		}
	}

	public class ZsetOperation extends DelOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			Collection<ScoredValue<String>> zset = (Collection<ScoredValue<String>>) ds.getValue();
			if (!zset.isEmpty()) {
				futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(ds.getKey(),
						zset.toArray(new ScoredValue[0])));
			}
		}
	}

	public class StreamOperation extends DelOperation<K, V> {

		private Converter<StreamMessage<K, V>, XAddArgs> xaddArgs = m -> null;

		public void setXaddArgs(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
			this.xaddArgs = xaddArgs;
		}

		public void setXaddArgsIdentity() {
			this.xaddArgs = m -> new XAddArgs().id(m.getId());
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
			batches((List<StreamMessage<K, V>>) ds.getValue()).forEach(b -> {
				List<Future<?>> streamFutures = new ArrayList<>();
				for (StreamMessage<K, V> message : b) {
					streamFutures.add(streamCommands.xadd(ds.getKey(), xaddArgs.convert(message), message.getBody()));
				}
				flush(commands, streamFutures);
			});
		}

		private <T> Stream<List<T>> batches(List<T> source) {
			int size = source.size();
			if (size <= 0) {
				return Stream.empty();
			}
			int fullChunks = (size - 1) / chunkSize;
			return IntStream.range(0, fullChunks + 1)
					.mapToObj(n -> source.subList(n * chunkSize, n == fullChunks ? size : (n + 1) * chunkSize));
		}

		private void flush(BaseRedisAsyncCommands<K, V> commands, List<Future<?>> futures) {
			flush(commands, futures.toArray(new RedisFuture[0]));
		}

		private void flush(BaseRedisAsyncCommands<K, V> commands, RedisFuture<?>... futures) {
			commands.flushCommands();
			log.trace("Executing {} commands", futures.length);
			boolean result = LettuceFutures.awaitAll(timeout.toMillis(), TimeUnit.MILLISECONDS, futures);
			if (result) {
				log.trace("Successfully executed {} commands", futures.length);
			} else {
				log.warn("Could not execute {} commands", futures.length);
			}
		}

	}

	public class TimeSeriesOperation implements DataStructureOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public void execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			List<Sample> samples = (List<Sample>) ds.getValue();
			for (Sample sample : samples) {
				futures.add(((RedisTimeSeriesAsyncCommands<K, V>) commands).tsAdd(ds.getKey(), sample));
			}
		}
	}

	public class JsonOperation implements DataStructureOperation<K, V> {

		private final K path;

		public JsonOperation(K path) {
			this.path = path;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisJSONAsyncCommands<K, V>) commands).jsonSet(ds.getKey(), path, (V) ds.getValue()));
		}
	}

	public class StringOperation implements DataStructureOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public void execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisStringAsyncCommands<K, V>) commands).set(ds.getKey(), (V) ds.getValue()));
		}
	}

	public void setXaddArgs(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
		streamOperation.setXaddArgs(xaddArgs);
	}

}
