package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConnectionPooling {

    public static void main(String[] args) throws Exception {

// Connection pool initialization
RedisURI redisURI = RedisURI.builder().build();
RedisClient client = RedisClient.create(redisURI);
GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
poolConfig.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);
poolConfig.setMinIdle(GenericObjectPoolConfig.DEFAULT_MIN_IDLE);
poolConfig.setTestOnReturn(BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
GenericObjectPool<StatefulRedisConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig);

// Each processing thread can tap into the pool
int pipelineSize = 50;
try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
    RedisAsyncCommands<String, String> commands = connection.async();
    commands.setAutoFlushCommands(false); // disable auto-flushing
    List<RedisFuture<?>> futures = new ArrayList<>(); // perform a series of independent calls
    for (int i = 0; i < pipelineSize; i++) {
        futures.add(commands.set("key-" + i, "value-" + i));
        futures.add(commands.expire("key-" + i, 3600));
    }
    commands.flushCommands(); // write all commands to the transport layer
    for (RedisFuture<?> future : futures) {
        future.get(redisURI.getTimeout().getSeconds(), TimeUnit.SECONDS); // synchronization example: Wait for each future to complete
    }
}
    }
}
