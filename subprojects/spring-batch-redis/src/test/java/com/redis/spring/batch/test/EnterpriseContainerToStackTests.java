package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class EnterpriseContainerToStackTests extends AbstractModulesTests {

	private static final RedisEnterpriseContainer SOURCE = RedisContainerFactory.enterprise();

	private static final RedisStackContainer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisEnterpriseContainer getRedisServer() {
		return SOURCE;
	}

	@Override
	protected RedisStackContainer getTargetRedisServer() {
		return TARGET;
	}

}
