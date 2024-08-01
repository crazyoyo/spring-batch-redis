package com.redis.spring.batch.test;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisStackContainer;

public interface RedisContainerFactory {

	static RedisStackContainer stack() {
		return new RedisStackContainer(RedisStackContainer.DEFAULT_IMAGE_NAME.withTag("7.2.0-v11"));
	}

	@SuppressWarnings("resource")
	static RedisEnterpriseContainer enterprise() {
		return new RedisEnterpriseContainer(
				RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG))
				.withDatabase(Database.builder().name("BatchTests").memoryMB(90).ossCluster(true)
						.modules(RedisModule.JSON, RedisModule.TIMESERIES, RedisModule.SEARCH).build());
	}

	static RedisContainer redis(String tag) {
		return new RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME.withTag(tag));
	}

}
