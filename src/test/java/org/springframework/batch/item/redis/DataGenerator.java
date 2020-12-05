package org.springframework.batch.item.redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Builder;
import lombok.Getter;

public class DataGenerator implements Runnable {

	private final RedisClient client;
	private final int start;
	private final int end;
	private final long sleep;
	@Getter
	private boolean finished;

	@Builder
	public DataGenerator(RedisClient client, int start, int end, long sleep) {
		this.client = client;
		this.start = start;
		this.end = end;
		this.sleep = sleep;
	}

	@Override
	public void run() {
		Random random = new Random();
		StatefulRedisConnection<String, String> connection = client.connect();
		try {
			RedisAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = new ArrayList<>();
			for (int index = start; index < end; index++) {
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
				if (sleep > 0) {
					Thread.sleep(sleep);
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
