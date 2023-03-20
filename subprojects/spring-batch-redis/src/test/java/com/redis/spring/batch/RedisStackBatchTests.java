package com.redis.spring.batch;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class RedisStackBatchTests extends AbstractBatchTests {

	private static final RedisStackContainer SOURCE = RedisContainerFactory.stack();
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
