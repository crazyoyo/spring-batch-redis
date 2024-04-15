package com.redis.spring.batch.test;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisStackContainer;

public interface RedisContainerFactory {

	static RedisStackContainer stack() {
		return new RedisStackContainer(RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));
	}

	@SuppressWarnings("resource")
	static RedisEnterpriseContainer enterprise() {
		return new RedisEnterpriseContainer(RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
				.withDatabase(Database.builder().name("BatchTests").memoryMB(90).ossCluster(true)
						.modules(RedisModule.JSON, RedisModule.TIMESERIES, RedisModule.SEARCH).build());
	}

	static RedisContainer redis(String tag) {
		return new RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME.withTag(tag));
	}

}
