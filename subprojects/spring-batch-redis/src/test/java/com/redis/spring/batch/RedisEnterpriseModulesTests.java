package com.redis.spring.batch;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledOnOs(OS.LINUX)
class RedisEnterpriseModulesTests extends AbstractModulesTests {

	protected static final RedisEnterpriseContainer source = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("BatchTests").memory(DataSize.ofMegabytes(50)).ossCluster(true)
					.modules(RedisModule.JSON, RedisModule.TIMESERIES).build());

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
