package com.redis.spring.batch;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

class Redis7Redis6Tests extends AbstractReplicationTests {

    private static final RedisContainer SOURCE = RedisContainerFactory.redis("7.0");

    private static final RedisContainer TARGET = RedisContainerFactory.redis("6.2.13");

    @Override
    protected RedisServer getSourceServer() {
        return SOURCE;
    }

    @Override
    protected RedisServer getTargetServer() {
        return TARGET;
    }

}
