package com.redis.spring.batch.test;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisServer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class REServerStackBatchTests extends BatchTests {

	private static final RedisServer source = new RedisEnterpriseServer()
			.withDatabase(Database.builder().shardCount(2).port(12001).ossCluster(true)
					.modules(RedisModule.JSON, RedisModule.SEARCH, RedisModule.TIMESERIES).build());

	private static final RedisServer target = RedisContainerFactory.stack();

	@Override
	public RedisServer getRedisServer() {
		return source;
	}

	@Override
	protected RedisServer getTargetRedisServer() {
		return target;
	}

}
