package com.redis.spring.batch.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.spring.batch.support.DataStructure.Type;
import com.redis.spring.batch.support.generator.Generator;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

class GeneratorTests extends AbstractRedisTestBase {

	@ParameterizedTest
	@RedisTestContextsSource
	void testDefaults(RedisTestContext context) throws Exception {
		execute(dataGenerator(context, "defaults"));
		RedisModulesCommands<String, String> sync = context.sync();
		long expectedCount = Generator.DEFAULT_SEQUENCE.getMaximum() - Generator.DEFAULT_SEQUENCE.getMinimum();
		long actualStringCount = sync.keys("string:*").size();
		Assertions.assertEquals(expectedCount, actualStringCount);
		Assertions.assertEquals(expectedCount * Type.values().length, sync.dbsize());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testToOption(RedisTestContext context) throws Exception {
		int count = 123;
		execute(dataGenerator(context, "to-options").end(count));
		RedisModulesCommands<String, String> sync = context.sync();
		int actualStringCount = sync.keys("string:*").size();
		Assertions.assertEquals(count, actualStringCount);
		Assertions.assertEquals(count * Type.values().length, sync.dbsize());
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.scard("set:100"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.llen("list:101"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.zcard("zset:102"));
		Assertions.assertEquals(Generator.DEFAULT_COLLECTION_CARDINALITY.getMinimum(), sync.xlen("stream:103"));
	}

}
