package org.springframework.batch.item.redis;

import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import redis.embedded.RedisServer;

public class BaseTest {

    public final static int REDIS_PORT = 16379;
    public final static int TARGET_REDIS_PORT = 16380;

    private static RedisServer server;
    private static RedisServer targetServer;

    @Autowired
    private RedisClient client;
    @Autowired
    private RedisClient targetClient;

    @BeforeAll
    public static void setup() {
        server = RedisServer.builder().port(REDIS_PORT).setting("notify-keyspace-events AK").build();
        server.start();
        targetServer = RedisServer.builder().port(TARGET_REDIS_PORT).build();
        targetServer.start();
    }

    @BeforeEach
    public void flushAll() {
        client.connect().sync().flushall();
        targetClient.connect().sync().flushall();
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
