package org.springframework.batch.item.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataPopulator implements Runnable {

    private final RedisClient client;

    private final int start;

    private final int end;

    private final Long sleep;

    @Getter
    private boolean finished;

    @Builder
    public DataPopulator(RedisClient client, int start, int end, Long sleep) {
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
	    RedisCommands<String, String> commands = connection.sync();
	    for (int index = start; index < end; index++) {
		String stringKey = "string:" + index;
		commands.set(stringKey, "value:" + index);
		commands.expireat(stringKey, System.currentTimeMillis() + random.nextInt(100000));
		Map<String, String> hash = new HashMap<>();
		hash.put("field1", "value" + index);
		hash.put("field2", "value" + index);
		commands.hmset("hash:" + index, hash);
		commands.sadd("set:" + (index % 10), "member:" + index);
		commands.zadd("zset:" + (index % 10), index % 3, "member:" + index);
		commands.xadd("stream:" + (index % 10), hash);
		if (sleep == null) {
		    continue;
		}
		try {
		    Thread.sleep(sleep);
		} catch (InterruptedException e) {
		    log.error("Interrupted", e);
		}
	    }
	    this.finished = true;
	} finally {
	    connection.close();
	}
    }

}
