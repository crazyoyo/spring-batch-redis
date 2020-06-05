package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.Architecture;
import redis.embedded.util.OS;

public class BaseTest {

    public final static int REDIS_PORT = 16379;
    public final static int TARGET_REDIS_PORT = 16380;

    private static RedisServer server;
    private static RedisServer targetServer;

    @Autowired
    private RedisClient redisClient;
    @Autowired
    private RedisClient targetRedisClient;

    @BeforeAll
    public static void setup() {
        RedisExecProvider customProvider = RedisExecProvider.defaultProvider()
                .override(OS.MAC_OS_X, Architecture.x86_64, "/usr/local/bin/redis-server");
        server = RedisServer.builder().redisExecProvider(customProvider).port(REDIS_PORT).setting("notify-keyspace-events AK").build();
        server.start();
        targetServer = RedisServer.builder().redisExecProvider(customProvider).port(TARGET_REDIS_PORT).build();
        targetServer.start();
    }

    @BeforeEach
    public void flushAll() {
        redisClient.connect().sync().flushall();
        targetRedisClient.connect().sync().flushall();
    }

    @AfterAll
    public static void teardown() {
        if (server != null) {
            server.stop();
        }
        if (targetServer != null) {
            targetServer.stop();
        }
    }

}
