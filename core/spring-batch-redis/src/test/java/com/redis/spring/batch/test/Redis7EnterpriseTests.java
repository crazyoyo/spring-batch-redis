package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class Redis7EnterpriseTests extends ReplicationTests {

    private static final RedisContainer SOURCE = RedisContainerFactory.redis("7.0");

    private static final RedisEnterpriseContainer TARGET = RedisContainerFactory.enterprise();

    @Override
    protected RedisServer getRedisServer() {
        return SOURCE;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return TARGET;
    }

}
