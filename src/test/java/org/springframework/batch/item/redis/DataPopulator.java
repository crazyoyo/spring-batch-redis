package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DataPopulator implements Runnable {
    private static final long EXPIRE_TIME = System.currentTimeMillis() / 1000 + 600;

    private final int start;
    private final int end;
    private final RedisClient redisClient;
    private final Long sleep;
    @Getter
    private boolean finished;

    @Builder
    public DataPopulator(RedisClient redisClient, int start, int end, Long sleep) {
        this.redisClient = redisClient;
        this.start = start;
        this.end = end;
        this.sleep = sleep;
    }

    @Override
    public void run() {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> commands = connection.sync();
        for (int index = start; index < end; index++) {
            String stringKey = "string:" + index;
            commands.set(stringKey, "value:" + index);
//            commands.expireat(stringKey, EXPIRE_TIME);
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
    }
}
