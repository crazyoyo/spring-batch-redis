package org.springframework.batch.item.redis.support.operation.executor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;

public class DataStructureOperationExecutor implements OperationExecutor<String, String, DataStructure> {

	private final long timeout;
	private final Converter<StreamMessage<String, String>, XAddArgs> xAddArgs;

	public DataStructureOperationExecutor(Duration timeout,
			Converter<StreamMessage<String, String>, XAddArgs> xAddArgs) {
		Assert.notNull(timeout, "A timeout duration is required");
		Assert.isTrue(!timeout.isNegative() && !timeout.isZero(), "Timeout duration must be positive");
		this.timeout = timeout.toMillis();
		this.xAddArgs = xAddArgs;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<RedisFuture<?>> execute(RedisModulesAsyncCommands<String, String> commands,
			List<? extends DataStructure> items) {
		List<RedisFuture<?>> futures = new ArrayList<>();
		for (DataStructure ds : items) {
			if (ds == null) {
				continue;
			}
			if (ds.getValue() == null) {
				futures.add(((RedisKeyAsyncCommands<String, String>) commands).del(ds.getKey()));
				continue;
			}
			if (ds.getType() == null) {
				continue;
			}
			switch (ds.getType().toLowerCase()) {
			case DataStructure.HASH:
				futures.add(commands.del(ds.getKey()));
				futures.add(commands.hset(ds.getKey(), (Map<String, String>) ds.getValue()));
				break;
			case DataStructure.STRING:
				futures.add(commands.set(ds.getKey(), (String) ds.getValue()));
				break;
			case DataStructure.LIST:
				flush(commands, commands.del(ds.getKey()),
						commands.rpush(ds.getKey(), ((Collection<String>) ds.getValue()).toArray(new String[0])));
				break;
			case DataStructure.SET:
				flush(commands, commands.del(ds.getKey()),
						commands.sadd(ds.getKey(), ((Collection<String>) ds.getValue()).toArray(new String[0])));
				break;
			case DataStructure.ZSET:
				flush(commands, commands.del(ds.getKey()), commands.zadd(ds.getKey(),
						((Collection<ScoredValue<String>>) ds.getValue()).toArray(new ScoredValue[0])));
				break;
			case DataStructure.STREAM:
				List<RedisFuture<?>> streamFutures = new ArrayList<>();
				streamFutures.add(commands.del(ds.getKey()));
				Collection<StreamMessage<String, String>> messages = (Collection<StreamMessage<String, String>>) ds
						.getValue();
				for (StreamMessage<String, String> message : messages) {
					streamFutures.add(commands.xadd(ds.getKey(), xAddArgs.convert(message), message.getBody()));
				}
				flush(commands, streamFutures.toArray(new RedisFuture[0]));
				break;
			}
			if (ds.getAbsoluteTTL() > 0) {
				futures.add(commands.pexpireat(ds.getKey(), ds.getAbsoluteTTL()));
			}
		}
		return futures;
	}

	private void flush(RedisModulesAsyncCommands<String, String> commands, RedisFuture<?>... futures) {
		commands.flushCommands();
		LettuceFutures.awaitAll(timeout, TimeUnit.MILLISECONDS, futures);
	}

}
