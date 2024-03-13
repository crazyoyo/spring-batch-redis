package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class EnterpriseServerToStackTests extends BatchTests {

	private static final RedisEnterpriseServer source = new RedisEnterpriseServer()
			.withDatabase(Database.builder().shardCount(2).port(12001).ossCluster(true)
					.modules(RedisModule.JSON, RedisModule.SEARCH, RedisModule.TIMESERIES).build());

	private static final RedisStackContainer target = RedisContainerFactory.stack();

	@Override
	public RedisServer getRedisServer() {
		return source;
	}

	@Override
	protected RedisStackContainer getTargetRedisServer() {
		return target;
	}

}
