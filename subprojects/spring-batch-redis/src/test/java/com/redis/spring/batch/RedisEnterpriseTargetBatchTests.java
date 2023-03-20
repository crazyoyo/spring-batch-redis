package com.redis.spring.batch;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class RedisEnterpriseTargetBatchTests extends AbstractBatchTests {

	@Override
	protected RedisServer getSourceServer() {
		return RedisStackBatchTests.REDIS_STACK;
	}

	@Override
	protected RedisServer getTargetServer() {
		return RedisEnterpriseSourceBatchTests.REDIS_ENTERPRISE;
	}
}
