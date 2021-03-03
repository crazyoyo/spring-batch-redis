package org.springframework.batch.item.redis;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.DataType;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Slf4j
@Builder
public class DataGenerator implements Callable<Long> {


    private static final int DEFAULT_START = 0;
    private static final int DEFAULT_END = 1000;
    private static final boolean DEFAULT_EXPIRE = true;
    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final int DEFAULT_MAX_EXPIRE = 100000;

    @NonNull
    private final GenericObjectPool<StatefulRedisConnection<String, String>> pool;
    @Builder.Default
    private final int start = DEFAULT_START;
    @Builder.Default
    private final int end = DEFAULT_END;
    private final long sleep;
    @Builder.Default
    private final int maxExpire = DEFAULT_MAX_EXPIRE;
    @Builder.Default
    private final int batchSize = DEFAULT_BATCH_SIZE;
    @Singular
    private final Set<DataType> dataTypes;

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
                if (contains(DataType.STRING)) {
                    String stringKey = "string:" + index;
                    futures.add(commands.set(stringKey, "value:" + index));
                    if (maxExpire > 0) {
                        futures.add(commands.expireat(stringKey, System.currentTimeMillis() + random.nextInt(maxExpire)));
                    }
                }
                Map<String, String> hash = new HashMap<>();
                hash.put("field1", "value" + index);
                hash.put("field2", "value" + index);
                String member = "member:" + index;
                int collectionIndex = index % 10;
                if (contains(DataType.HASH)) {
                    futures.add(commands.hset("hash:" + index, hash));
                }
                if (contains(DataType.SET)) {
                    futures.add(commands.sadd("set:" + collectionIndex, member));
                }
                if (contains(DataType.ZSET)) {
                    futures.add(commands.zadd("zset:" + collectionIndex, index % 3, member));
                }
                if (contains(DataType.STREAM)) {
                    futures.add(commands.xadd("stream:" + collectionIndex, hash));
                }
                if (contains(DataType.LIST)) {
                    futures.add(commands.lpush("list:" + collectionIndex, member));
                }
                if (futures.size() >= batchSize) {
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

    private boolean contains(DataType type) {
        if (dataTypes.isEmpty()) {
            return true;
        }
        return dataTypes.contains(type);
    }


}
