package org.springframework.batch.item.redis;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisListAsyncCommands;
import io.lettuce.core.api.async.RedisSetAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.batch.item.redis.support.DataStructure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class DataGenerator implements Callable<Long> {

    private final Supplier<StatefulConnection<String, String>> connectionSupplier;
    private final Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async;
    private final int start;
    private final int end;
    private final long sleep;
    private final Duration minExpire;
    private final Duration maxExpire;
    private final int batchSize;
    private final Set<String> dataTypes;

    public DataGenerator(Supplier<StatefulConnection<String, String>> connectionSupplier, Function<StatefulConnection<String, String>, BaseRedisAsyncCommands<String, String>> async, int start, int end, long sleep, Duration minExpire, Duration maxExpire, int batchSize, Set<String> dataTypes) {
        this.connectionSupplier = connectionSupplier;
        this.async = async;
        this.start = start;
        this.end = end;
        this.sleep = sleep;
        this.minExpire = minExpire;
        this.maxExpire = maxExpire;
        this.batchSize = batchSize;
        this.dataTypes = dataTypes;
    }

    @Override
    public Long call() throws Exception {
        StatefulConnection<String, String> connection = connectionSupplier.get();
        BaseRedisAsyncCommands<String, String> commands = async.apply(connection);
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


    @SuppressWarnings("unchecked")
    private class CommandExecutor implements Callable<Long> {

        private final Random random = new Random();
        private final BaseRedisAsyncCommands<String, String> commands;
        private final List<RedisFuture<?>> futures = new ArrayList<>();

        public CommandExecutor(BaseRedisAsyncCommands<String, String> commands) {
            this.commands = commands;
        }

        @Override
        public Long call() throws InterruptedException {
            long count = 0;
            for (int index = start; index < end; index++) {
                if (contains(DataStructure.STRING)) {
                    String stringKey = "string:" + index;
                    futures.add(((RedisStringAsyncCommands<String, String>) commands).set(stringKey, "value:" + index));
                    if (!maxExpire.isZero()) {
                        long time = System.currentTimeMillis() + minExpire.toMillis() + random.nextInt(Math.toIntExact(maxExpire.toMillis()));
                        futures.add(((RedisKeyAsyncCommands<String, String>) commands).pexpireat(stringKey, time));
                    }
                }
                int collectionIndex = index % 10;
                if (contains(DataStructure.HASH)) {
                    futures.add(((RedisHashAsyncCommands<String, String>) commands).hset("hash:" + index, hash(index)));
                }
                if (contains(DataStructure.SET)) {
                    futures.add(((RedisSetAsyncCommands<String, String>) commands).sadd("set:" + collectionIndex, member(index)));
                }
                if (contains(DataStructure.ZSET)) {
                    futures.add(((RedisSortedSetAsyncCommands<String, String>) commands).zadd("zset:" + collectionIndex, index % 3, member(index)));
                }
                if (contains(DataStructure.STREAM)) {
                    futures.add(((RedisStreamAsyncCommands<String, String>) commands).xadd("stream:" + collectionIndex, hash(index)));
                }
                if (contains(DataStructure.LIST)) {
                    futures.add(((RedisListAsyncCommands<String, String>) commands).lpush("list:" + collectionIndex, member(index)));
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

        private String member(int index) {
            return "member:" + index;
        }

        private Map<String, String> hash(int index) {
            Map<String, String> hash = new HashMap<>();
            hash.put("field1", "value" + index);
            hash.put("field2", "value" + index);
            return hash;
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

    private boolean contains(String type) {
        if (dataTypes.isEmpty()) {
            return true;
        }
        return dataTypes.contains(type);
    }

    public static DataGeneratorBuilder client(RedisClient client) {
        return new DataGeneratorBuilder(client);
    }

    public static DataGeneratorBuilder client(RedisClusterClient client) {
        return new DataGeneratorBuilder(client);
    }

    @Setter
    @Accessors(fluent = true)
    public static class DataGeneratorBuilder extends CommandBuilder<String, String, DataGeneratorBuilder> {

        private static final int DEFAULT_START = 0;
        private static final int DEFAULT_END = 1000;
        private static final int DEFAULT_BATCH_SIZE = 50;
        private static final Duration DEFAULT_MIN_EXPIRE = Duration.ofSeconds(100);
        private static final Duration DEFAULT_MAX_EXPIRE = Duration.ofSeconds(1000);
        private static final long DEFAULT_SLEEP = 0;

        private int start = DEFAULT_START;
        private int end = DEFAULT_END;
        private long sleep = DEFAULT_SLEEP;
        private Duration minExpire = DEFAULT_MIN_EXPIRE;
        private Duration maxExpire = DEFAULT_MAX_EXPIRE;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private Set<String> dataTypes = new HashSet<>(Arrays.asList(DataStructure.HASH, DataStructure.LIST, DataStructure.STRING, DataStructure.STREAM, DataStructure.SET, DataStructure.ZSET));

        public DataGeneratorBuilder(RedisClusterClient client) {
            super(client, StringCodec.UTF8);
        }

        public DataGeneratorBuilder(RedisClient client) {
            super(client, StringCodec.UTF8);
        }

        public DataGeneratorBuilder dataTypes(String... dataTypes) {
            this.dataTypes = new HashSet<>(Arrays.asList(dataTypes));
            return this;
        }

        public DataGenerator build() {
            return new DataGenerator(connectionSupplier, async, start, end, sleep, minExpire, maxExpire, batchSize, new HashSet<>(dataTypes));
        }
    }

}
