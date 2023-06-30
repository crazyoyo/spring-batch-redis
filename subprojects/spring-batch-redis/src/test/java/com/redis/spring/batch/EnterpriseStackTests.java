package com.redis.spring.batch;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class EnterpriseStackTests extends AbstractTests {

	private static final RedisEnterpriseContainer SOURCE = RedisContainerFactory.enterprise();
	private static final RedisStackContainer TARGET = RedisContainerFactory.stack();

	@Override
	protected RedisServer getSourceServer() {
		return SOURCE;
	}

	@Override
	protected RedisServer getTargetServer() {
		return TARGET;
	}
}
