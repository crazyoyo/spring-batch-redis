package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class StackToEnterpriseServerTests extends AbstractModulesTests {

	private static final RedisStackContainer source = RedisContainerFactory.stack();

	private static final RedisEnterpriseServer target = new RedisEnterpriseServer()
			.withDatabase(Database.builder().shardCount(2).port(12001).ossCluster(true)
					.modules(RedisModule.JSON, RedisModule.SEARCH, RedisModule.TIMESERIES).build());

	@Override
	protected RedisStackContainer getRedisServer() {
		return source;
	}

	@Override
	protected RedisEnterpriseServer getTargetRedisServer() {
		return target;
	}

}
