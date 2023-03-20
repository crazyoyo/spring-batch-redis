package com.redis.spring.batch;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class RedisStackBatchTests extends AbstractBatchTests {

	public static final RedisStackContainer REDIS_STACK = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	public static final RedisStackContainer TARGET_REDIS_STACK = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected RedisServer getSourceServer() {
		return REDIS_STACK;
	}

	@Override
	protected RedisServer getTargetServer() {
		return TARGET_REDIS_STACK;
	}
}
