package com.redis.spring.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.support.generator.DataStructureGeneratorItemReader.Options;
import com.redis.spring.batch.support.generator.Generator.DataType;
import com.redis.testcontainers.RedisServer;

public class RedisGeneratorTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@MethodSource("servers")
	void testDefaults(RedisServer redis) throws Exception {
		dataGenerator("defaults", redis).build().call();
		RedisModulesCommands<String, String> sync = sync(redis);
		int expectedCount = Options.DEFAULT_END - Options.DEFAULT_START;
		int actualStringCount = sync.keys(DataType.STRING + ":*").size();
		Assertions.assertEquals(expectedCount, actualStringCount);
		Assertions.assertEquals(expectedCount * DataType.values().length, sync.dbsize());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testToOption(RedisServer redis) throws Exception {
		int count = 123;
		dataGenerator("to-options", redis).to(123).build().call();
		RedisModulesCommands<String, String> sync = sync(redis);
		int actualStringCount = sync.keys(DataType.STRING + ":*").size();
		Assertions.assertEquals(count, actualStringCount);
		Assertions.assertEquals(count * DataType.values().length, sync.dbsize());
	}

}
