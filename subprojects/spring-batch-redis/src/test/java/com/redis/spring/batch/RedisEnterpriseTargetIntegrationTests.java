package com.redis.spring.batch;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class RedisEnterpriseTargetIntegrationTests extends AbstractBatchIntegrationTests {

	@Override
	protected RedisServer getSourceServer() {
		return REDIS_STACK;
	}

	@Override
	protected RedisServer getTargetServer() {
		return REDIS_ENTERPRISE;
	}
}
