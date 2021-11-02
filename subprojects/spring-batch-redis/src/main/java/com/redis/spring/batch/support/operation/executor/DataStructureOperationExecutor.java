package com.redis.spring.batch.support.operation.executor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.support.DataStructure;
import com.redis.spring.batch.support.Utils;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
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

	private final long timeout;
	private final Converter<StreamMessage<K, V>, XAddArgs> xAddArgs;

	public DataStructureOperationExecutor(Duration timeout, Converter<StreamMessage<K, V>, XAddArgs> xAddArgs) {
		Utils.assertPositive(timeout, "Timeout duration");
		this.timeout = timeout.toMillis();
		this.xAddArgs = xAddArgs;
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
		if (ds.getType() == null) {
			return;
		}
		String lowerCase = ds.getType().toLowerCase();
		if (lowerCase.equals(DataStructure.HASH)) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			futures.add(((RedisHashAsyncCommands<K, V>) commands).hset(ds.getKey(), (Map<K, V>) ds.getValue()));
		} else if (lowerCase.equals(DataStructure.STRING)) {
			futures.add(((RedisStringAsyncCommands<K, V>) commands).set(ds.getKey(), (V) ds.getValue()));
		} else if (lowerCase.equals(DataStructure.LIST)) {
			flush(commands, ((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()),
					((RedisListAsyncCommands<K, V>) commands).rpush(ds.getKey(),
							(V[]) ((Collection<V>) ds.getValue()).toArray()));
		} else if (lowerCase.equals(DataStructure.SET)) {
			flush(commands, ((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()),
					((RedisSetAsyncCommands<K, V>) commands).sadd(ds.getKey(),
							(V[]) ((Collection<V>) ds.getValue()).toArray()));
		} else if (lowerCase.equals(DataStructure.ZSET)) {
			flush(commands, ((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()),
					((RedisSortedSetAsyncCommands<K, V>) commands).zadd(ds.getKey(),
							((Collection<ScoredValue<String>>) ds.getValue()).toArray(new ScoredValue[0])));
		} else if (lowerCase.equals(DataStructure.STREAM)) {
			List<RedisFuture<?>> streamFutures = new ArrayList<>();
			streamFutures.add(((RedisKeyAsyncCommands<K, V>) commands).del(ds.getKey()));
			Collection<StreamMessage<K, V>> messages = (Collection<StreamMessage<K, V>>) ds.getValue();
			for (StreamMessage<K, V> message : messages) {
				streamFutures.add(((RedisStreamAsyncCommands<K, V>) commands).xadd(ds.getKey(),
						xAddArgs.convert(message), message.getBody()));
			}
			flush(commands, streamFutures.toArray(new RedisFuture[0]));
		}
		if (ds.hasTTL()) {
			futures.add(((RedisKeyAsyncCommands<K, V>) commands).pexpireat(ds.getKey(), ds.getAbsoluteTTL()));
		}
	}

	private void flush(BaseRedisAsyncCommands<K, V> commands, RedisFuture<?>... futures) {
		commands.flushCommands();
		LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures);
	}

}
