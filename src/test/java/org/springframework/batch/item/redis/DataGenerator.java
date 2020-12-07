package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.springframework.batch.item.redis.support.ClientUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;

@Builder
public class DataGenerator implements Runnable {

	private final AbstractRedisClient client;
	@Default
	private final DataGeneratorOptions options = DataGeneratorOptions.builder().build();
	@Getter
	private boolean finished = false;

	public static DataGeneratorBuilder builder(AbstractRedisClient client) {
		return new DataGeneratorBuilder().client(client);
	}

	@Override
	public void run() {
		Random random = new Random();
		Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async = ClientUtils
				.async(client);
		StatefulConnection<String, String> connection = ClientUtils.connection(client);
		try {
			RedisAsyncCommands<String, String> commands = (RedisAsyncCommands<String, String>) async.apply(connection);
			commands.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = new ArrayList<>();
			for (int index = options.getStart(); index < options.getEnd(); index++) {
				String stringKey = "string:" + index;
				futures.add(commands.set(stringKey, "value:" + index));
				futures.add(commands.expireat(stringKey, System.currentTimeMillis() + random.nextInt(100000)));
				Map<String, String> hash = new HashMap<>();
				hash.put("field1", "value" + index);
				hash.put("field2", "value" + index);
				futures.add(commands.hmset("hash:" + index, hash));
				futures.add(commands.sadd("set:" + (index % 10), "member:" + index));
				futures.add(commands.zadd("zset:" + (index % 10), index % 3, "member:" + index));
				futures.add(commands.xadd("stream:" + (index % 10), hash));
				if (futures.size() >= 50) {
					commands.flushCommands();
					LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
					futures.clear();
				}
				if (options.getSleep() > 0) {
					Thread.sleep(options.getSleep());
				}
			}
			commands.flushCommands();
			LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
			futures.clear();
			this.finished = true;
		} catch (InterruptedException e) {
			// ignore
		} finally {
			connection.close();
		}
	}

}
