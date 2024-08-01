package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class StackREContainerBatchTests extends BatchTests {

	private static final RedisServer source = RedisContainerFactory.stack();

	private static final RedisServer target = RedisContainerFactory.enterprise();

	@Override
	protected RedisServer getRedisServer() {
		return source;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

}
