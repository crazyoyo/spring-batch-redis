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

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHLLAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;

public class DataStructureOperationExecutor<K, V> implements OperationExecutor<K, V, DataStructure<K>> {

	private static final Logger log = LoggerFactory.getLogger(DataStructureOperationExecutor.class);

	private static final int DEFAULT_BATCH_SIZE = 50;

	private Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;
	private Converter<StreamMessage<K, V>, XAddArgs> xaddArgs = m -> new XAddArgs().id(m.getId());
	private int batchSize = DEFAULT_BATCH_SIZE;
	private DataStructureOperation<K, V> hashOperation = new HashOperation<>();
	private DataStructureOperation<K, V> listOperation = new ListOperation<>();
	private DataStructureOperation<K, V> setOperation = new SetOperation<>();
	private DataStructureOperation<K, V> streamOperation = new StreamOperation();
	private DataStructureOperation<K, V> stringOperation = new StringOperation<>();
	private DataStructureOperation<K, V> zsetOperation = new ZsetOperation<>();

	public void setHashOperation(DataStructureOperation<K, V> hashOperation) {
		this.hashOperation = hashOperation;
	}

	public void setListOperation(DataStructureOperation<K, V> listOperation) {
		this.listOperation = listOperation;
	}

	public void setSetOperation(DataStructureOperation<K, V> setOperation) {
		this.setOperation = setOperation;
	}

	public void setStreamOperation(DataStructureOperation<K, V> streamOperation) {
		this.streamOperation = streamOperation;
	}

	public void setStringOperation(DataStructureOperation<K, V> stringOperation) {
		this.stringOperation = stringOperation;
	}

	public void setZsetOperation(DataStructureOperation<K, V> zsetOperation) {
		this.zsetOperation = zsetOperation;
	}

	public void setTimeout(Duration timeout) {
		Utils.assertPositive(timeout, "Timeout duration");
		this.timeout = timeout;
	}

	public void setXaddArgs(Converter<StreamMessage<K, V>, XAddArgs> xaddArgs) {
		this.xaddArgs = xaddArgs;
	}

	public void setBatchSize(int batchSize) {
		Utils.assertPositive(batchSize, "Batch size");
		this.batchSize = batchSize;
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
		switch (ds.getType()) {
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
		case NONE:
			break;
		}
		if (ds.hasTTL()) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(ds.getKey(), ds.getAbsoluteTTL()));
		}
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

	public static class HashOperation<K, V> extends DelOperation<K, V> {
		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(ds.getKey(), (Map<K, V>) ds.getValue()));
		}
	}

	public static class ListOperation<K, V> extends DelOperation<K, V> {
		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisListAsyncCommands<K, V>) commands).rpush(ds.getKey(),
					(V[]) ((Collection<V>) ds.getValue()).toArray()));
		}
	}

	public static class SetOperation<K, V> extends DelOperation<K, V> {
		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisSetAsyncCommands<K, V>) commands).sadd(ds.getKey(),
					(V[]) ((Collection<V>) ds.getValue()).toArray()));

		}
	}

	public static class ZsetOperation<K, V> extends DelOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		protected void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisSortedSetAsyncCommands<K, V>) commands).zadd(ds.getKey(),
					((Collection<ScoredValue<String>>) ds.getValue()).toArray(new ScoredValue[0])));
		}
	}

	private class StreamOperation extends DelOperation<K, V> {

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
			int fullChunks = (size - 1) / batchSize;
			return IntStream.range(0, fullChunks + 1)
					.mapToObj(n -> source.subList(n * batchSize, n == fullChunks ? size : (n + 1) * batchSize));
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

	public static class StringOperation<K, V> implements DataStructureOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public void execute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisStringAsyncCommands<K, V>) commands).set(ds.getKey(), (V) ds.getValue()));
		}
	}

	public static class PfaddStringOperation<K, V> extends DelOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisModulesAsyncCommands<K, V>) commands).pfaddNoValue(ds.getKey()));
			futures.add(((RedisStringAsyncCommands<K, V>) commands).set(ds.getKey(), (V) ds.getValue()));
		}
	}

	public static class PfaddMembersOperation<K, V> extends DelOperation<K, V> {

		@SuppressWarnings("unchecked")
		@Override
		public void doExecute(BaseRedisAsyncCommands<K, V> commands, DataStructure<K> ds, List<Future<?>> futures) {
			futures.add(((RedisHLLAsyncCommands<K, V>) commands).pfadd(ds.getKey(),
					(V[]) ((Collection<V>) ds.getValue()).toArray()));
		}
	}

}
