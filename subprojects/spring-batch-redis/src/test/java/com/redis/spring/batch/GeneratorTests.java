package com.redis.spring.batch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.testcontainers.RedisServer;

public class GeneratorTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@MethodSource("servers")
	public void testDefaults(RedisServer redis) throws Exception {
		execute(dataGenerator(redis, "defaults"));
		RedisModulesCommands<String, String> sync = sync(redis);
		long expectedCount = Generator.DEFAULT_SEQUENCE.getMaximum() - Generator.DEFAULT_SEQUENCE.getMinimum();
		long actualStringCount = sync.keys("string:*").size();
		Assertions.assertEquals(expectedCount, actualStringCount);
		Assertions.assertEquals(expectedCount * Type.values().length, sync.dbsize());
	}

	@ParameterizedTest
	@MethodSource("servers")
	public void testToOption(RedisServer redis) throws Exception {
		int count = 123;
		execute(dataGenerator(redis, "to-options").end(count));
		RedisModulesCommands<String, String> sync = sync(redis);
		int actualStringCount = sync.keys("string:*").size();
		Assertions.assertEquals(count, actualStringCount);
		Assertions.assertEquals(count * Type.values().length, sync.dbsize());
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.scard("set:100"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.llen("list:101"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.zcard("zset:102"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.xlen("stream:103"));
	}

}
