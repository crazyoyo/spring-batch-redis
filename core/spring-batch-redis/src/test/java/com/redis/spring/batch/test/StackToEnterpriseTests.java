package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class StackToEnterpriseTests extends ModulesTests {

	private static final RedisStackContainer SOURCE = RedisContainerFactory.stack();

	private static final RedisEnterpriseContainer TARGET = RedisContainerFactory.enterprise();

	@Override
	protected RedisStackContainer getRedisContainer() {
		return SOURCE;
	}

	@Override
	protected RedisEnterpriseContainer getTargetRedisContainer() {
		return TARGET;
	}

}
