package com.redis.spring.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.support.generator.DataStructureGeneratorItemReader.DataStructureOptions;
import com.redis.spring.batch.support.generator.Generator.DataType;
import com.redis.testcontainers.RedisServer;

public class RedisGeneratorTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@MethodSource("servers")
	void testDefaults(RedisServer redis) throws Exception {
		dataGenerator(redis, "defaults").build().call();
		RedisModulesCommands<String, String> sync = sync(redis);
		int expectedCount = DataStructureOptions.DataStructureOptionsBuilder.DEFAULT_END
				- DataStructureOptions.DataStructureOptionsBuilder.DEFAULT_START;
		int actualStringCount = sync.keys(DataType.STRING + ":*").size();
		Assertions.assertEquals(expectedCount, actualStringCount);
		Assertions.assertEquals(expectedCount * DataType.values().length, sync.dbsize());
	}

	@ParameterizedTest
	@MethodSource("servers")
	void testToOption(RedisServer redis) throws Exception {
		int count = 123;
		dataGenerator(redis, "to-options").end(123).build().call();
		RedisModulesCommands<String, String> sync = sync(redis);
		int actualStringCount = sync.keys(DataType.STRING + ":*").size();
		Assertions.assertEquals(count, actualStringCount);
		Assertions.assertEquals(count * DataType.values().length, sync.dbsize());
	}

}
