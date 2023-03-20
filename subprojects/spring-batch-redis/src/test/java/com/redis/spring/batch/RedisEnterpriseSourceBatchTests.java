package com.redis.spring.batch;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class RedisEnterpriseSourceBatchTests extends AbstractBatchTests {

	public static final RedisEnterpriseContainer REDIS_ENTERPRISE = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("BatchTests").memory(DataSize.ofMegabytes(50)).ossCluster(true)
					.modules(RedisModule.JSON, RedisModule.TIMESERIES, RedisModule.SEARCH).build());

	@Override
	protected RedisServer getSourceServer() {
		return REDIS_ENTERPRISE;
	}

	@Override
	protected RedisServer getTargetServer() {
		return RedisStackBatchTests.REDIS_STACK;
	}
}
