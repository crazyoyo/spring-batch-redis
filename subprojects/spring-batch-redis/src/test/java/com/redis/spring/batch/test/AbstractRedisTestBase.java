package com.redis.spring.batch.test;

import java.util.Arrays;
import java.util.Collection;

import org.testcontainers.junit.jupiter.Container;

import com.redis.testcontainers.RedisClusterContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

public abstract class AbstractRedisTestBase extends AbstractTestBase {

	@Container
	protected static final RedisContainer REDIS = new RedisContainer().withKeyspaceNotifications();
	@Container
	protected static final RedisClusterContainer REDIS_CLUSTER = new RedisClusterContainer()
			.withKeyspaceNotifications();

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDIS, REDIS_CLUSTER);
	}

}
