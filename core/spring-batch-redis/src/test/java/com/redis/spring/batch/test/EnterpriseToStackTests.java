package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class EnterpriseToStackTests extends ModulesTests {

	private static final RedisEnterpriseContainer SOURCE = RedisContainerFactory.enterprise();

	private static final RedisStackContainer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisEnterpriseContainer getRedisContainer() {
		return SOURCE;
	}

	@Override
	protected RedisStackContainer getTargetRedisContainer() {
		return TARGET;
	}

}
