package com.redis.spring.batch.support.operation.executor;

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

import com.redis.spring.batch.support.DataStructure;
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

public class DataStructureOperationExecutor<K, V> implements OperationExecutor<K, V, DataStructure<K>> {

	private static final Logger log = LoggerFactory.getLogger(DataStructureOperationExecutor.class);

	private static final int DEFAULT_BATCH_SIZE = 50;

	private Duration timeout = RedisURI.DEFAULT_TIMEOUT_DURATION;
	private Converter<StreamMessage<K, V>, XAddArgs> xaddArgs = m -> new XAddArgs().id(m.getId());
	private int batchSize = DEFAULT_BATCH_SIZE;

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
	public List<Future<?>> execute(BaseRedisAsyncCommands<K, V> commands, List<? extends DataStructure<K>> items) {
		List<Future<?>> futures = new ArrayList<>();
		for (DataStructure<K> item : items) {
			execute(commands, futures, item);
		}
		return futures;
	}

	@SuppressWarnings("unchecked")
	private void execute(BaseRedisAsyncCommands<K, V> commands, List<Future<?>> futures, DataStructure<K> ds) {
		if (ds == null) {
			return;
		}
		if (ds.getValue() == null) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			return;
		}
		switch (ds.getType()) {
		case HASH:
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(ds.getKey(), (Map<K, V>) ds.getValue()));
			break;
		case STRING:
			V string = (V) ds.getValue();
			futures.add(((RedisStringAsyncCommands<K, V>) commands).set(ds.getKey(), string));
			break;
		case LIST:
			flush(commands, ((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()),
					((RedisListAsyncCommands<K, V>) commands).rpush(ds.getKey(),
							(V[]) ((Collection<V>) ds.getValue()).toArray()));
			break;
		case SET:
			flush(commands, ((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()),
					((RedisSetAsyncCommands<K, V>) commands).sadd(ds.getKey(),
							(V[]) ((Collection<V>) ds.getValue()).toArray()));
			break;
		case ZSET:
			flush(commands, ((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()),
					((RedisSortedSetAsyncCommands<K, V>) commands).zadd(ds.getKey(),
							((Collection<ScoredValue<String>>) ds.getValue()).toArray(new ScoredValue[0])));
			break;
		case STREAM:
			RedisStreamAsyncCommands<K, V> streamCommands = (RedisStreamAsyncCommands<K, V>) commands;
			flush(commands, ((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			batches((List<StreamMessage<K, V>>) ds.getValue()).forEach(b -> {
				List<RedisFuture<?>> streamFutures = new ArrayList<>();
				for (StreamMessage<K, V> message : b) {
					streamFutures.add(streamCommands.xadd(ds.getKey(), xaddArgs.convert(message), message.getBody()));
				}
				flush(commands, streamFutures);
			});
			break;
		case NONE:
			break;
		}
		if (ds.hasTTL()) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(ds.getKey(), ds.getAbsoluteTTL()));
		}
	}

	public <T> Stream<List<T>> batches(List<T> source) {
		int size = source.size();
		if (size <= 0) {
			return Stream.empty();
		}
		int fullChunks = (size - 1) / batchSize;
		return IntStream.range(0, fullChunks + 1)
				.mapToObj(n -> source.subList(n * batchSize, n == fullChunks ? size : (n + 1) * batchSize));
	}

	private void flush(BaseRedisAsyncCommands<K, V> commands, List<RedisFuture<?>> futures) {
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
