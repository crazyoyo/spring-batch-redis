package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class REContainerStackBatchTests extends BatchTests {

	private static final RedisEnterpriseContainer source = RedisContainerFactory.enterprise();

	private static final RedisServer target = RedisContainerFactory.stack();

	@Override
	protected RedisServer getRedisServer() {
		return source;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

}
