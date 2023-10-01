package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.utility.DockerImageName;

import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(value = OS.MAC)
public class ClusterToRedisTests extends BatchTests {

    private static final RedisContainer redis = new RedisContainer(
            RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

    private static final RedisClusterContainer cluster = new RedisClusterContainer(
            DockerImageName.parse("neohq/redis-cluster").withTag("latest"));

    @Override
    protected RedisServer getRedisServer() {
        return cluster;
    }

    @Override
    protected RedisServer getTargetRedisServer() {
        return redis;
    }

}
