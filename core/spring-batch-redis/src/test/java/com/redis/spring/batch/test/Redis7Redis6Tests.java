package com.redis.spring.batch.test;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

class Redis7Redis6Tests extends ReplicationTests {

    private static final RedisContainer SOURCE = RedisContainerFactory.redis("7.0");

    private static final RedisContainer TARGET = RedisContainerFactory.redis("6.2.13");

    @Override
    protected RedisServer getRedisServer() {
        return SOURCE;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return TARGET;
    }

}
