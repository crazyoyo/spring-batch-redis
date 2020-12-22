package org.springframework.batch.item.redis;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DataGenerator implements Callable<Long> {

    private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
    private final boolean expire;
    private final int start;
    private final int end;
    private final long sleep;

    public DataGenerator(GenericObjectPool<StatefulRedisConnection<String, String>> pool, int start, int end, long sleep, boolean expire) {
        this.pool = pool;
        this.start = start;
        this.end = end;
        this.sleep = sleep;
        this.expire = expire;
    }

    @Override
    public Long call() throws Exception {
        try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
            RedisAsyncCommands<String, String> commands = connection.async();
            commands.setAutoFlushCommands(false);
            long count = 0;
            CommandExecutor executor = new CommandExecutor(commands);
            try {
                count += executor.call();
            } finally {
                commands.setAutoFlushCommands(true);
            }
            return count;
        }
    }

    private class CommandExecutor implements Callable<Long> {

        private final Random random = new Random();
        private final RedisAsyncCommands<String, String> commands;
        private final List<RedisFuture<?>> futures = new ArrayList<>();

        public CommandExecutor(RedisAsyncCommands<String, String> commands) {
            this.commands = commands;
        }

        @Override
        public Long call() throws InterruptedException {
            long count = 0;
            for (int index = start; index < end; index++) {
                String stringKey = "string:" + index;
                futures.add(commands.set(stringKey, "value:" + index));
                if (expire) {
                    futures.add(commands.expireat(stringKey, System.currentTimeMillis() + random.nextInt(100000)));
                }
                Map<String, String> hash = new HashMap<>();
                hash.put("field1", "value" + index);
                hash.put("field2", "value" + index);
                futures.add(commands.hmset("hash:" + index, hash));
                futures.add(commands.sadd("set:" + (index % 10), "member:" + index));
                futures.add(commands.zadd("zset:" + (index % 10), index % 3, "member:" + index));
                futures.add(commands.xadd("stream:" + (index % 10), hash));
                if (futures.size() >= 50) {
                    count += flush();
                }
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
            count += flush();
            return count;
        }

        private int flush() {
            commands.flushCommands();
            LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
            try {
                return futures.size();
            } finally {
                futures.clear();
            }
        }
    }

    public static DataGeneratorBuilder builder(GenericObjectPool<StatefulRedisConnection<String, String>> pool) {
        return new DataGeneratorBuilder(pool);
    }

    @Setter
    @Accessors(fluent = true)
    public static class DataGeneratorBuilder {

        private static final int DEFAULT_START = 0;
        private static final int DEFAULT_END = 1000;
        private static final boolean DEFAULT_EXPIRE = true;

        private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
        private int start = DEFAULT_START;
        private int end = DEFAULT_END;
        private long sleep;
        private boolean expire = DEFAULT_EXPIRE;

        public DataGeneratorBuilder(GenericObjectPool<StatefulRedisConnection<String, String>> pool) {
            this.pool = pool;
        }

        public DataGenerator build() {
            Assert.notNull(pool, "A Redis connection pool is required.");
            return new DataGenerator(pool, start, end, sleep, expire);
        }

    }

}
