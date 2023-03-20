package com.redis.spring.batch;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class RedisEnterpriseSourceIntegrationTests extends AbstractBatchIntegrationTests {

	@Override
	protected RedisServer getSourceServer() {
		return REDIS_ENTERPRISE;
	}

	@Override
	protected RedisServer getTargetServer() {
		return REDIS_STACK;
	}
}
