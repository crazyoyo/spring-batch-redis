package com.redis.spring.batch;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class RedisStackIntegrationTests extends AbstractIntegrationTests {

	protected static final RedisStackContainer source = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	private static final RedisStackContainer target = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected RedisServer getSourceServer() {
		return source;
	}

	@Override
	protected RedisServer getTargetServer() {
		return target;
	}
}
