package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class StackToEnterpriseContainerTests extends ModulesTests {

	private static final RedisStackContainer SOURCE = RedisContainerFactory.stack();

	private static final RedisEnterpriseContainer TARGET = RedisContainerFactory.enterprise();

	@Override
	protected RedisStackContainer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisEnterpriseContainer getTargetRedisServer() {
		return TARGET;
	}

}
